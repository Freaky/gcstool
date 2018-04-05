use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::fs::{File, OpenOptions};
use std::{thread, time};
use std::time::Instant;

extern crate byteorder;
extern crate sha1;

#[macro_use]
extern crate clap;

mod bitio;
mod gcs;

use gcs::*;

const ESTIMATE_LIMIT: u64 = 1024 * 1024 * 16;

fn estimate_lines(mut inp: &std::fs::File) -> io::Result<u64> {
    let size = inp.metadata()?.len();
    let sample_size = std::cmp::min(size, ESTIMATE_LIMIT) as usize;

    let mut buffer: Vec<u8> = vec![0; sample_size];
    inp.read_exact(&mut buffer)?;
    inp.seek(SeekFrom::Start(0))?;

    let sample = buffer.iter().filter(|b| **b == b'\n').count() as u64;

    Ok(sample * (size / (sample_size as u64)))
}

fn u64_from_hex(src: &[u8]) -> Option<u64> {
    let mut result: u64 = 0;

    for &c in src {
        result = match result.checked_mul(16).and_then(|r| {
            (c as char)
                .to_digit(16)
                .and_then(|x| r.checked_add(u64::from(x)))
        }) {
            Some(result) => result,
            None => return None,
        }
    }

    Some(result)
}

fn query_gcs<R: io::Read + io::Seek>(test_in: R) {
    let test_inbuf = BufReader::new(test_in);
    let mut searcher = GCSReader::new(test_inbuf);
    searcher.initialize().expect("GCD initialize failure");

    let stdin = io::stdin();

    for line in stdin.lock().lines() {
        let mut sha = sha1::Sha1::new();
        let line = line.unwrap();
        println!("Search for '{}'", line);
        sha.update(line.as_bytes());
        let hash = sha.digest().to_string();
        let val = u64_from_hex(&hash.as_bytes()[0..15]).unwrap_or(0);
        let start = Instant::now();
        println!("Search: {:?}", searcher.exists(val));
        let elapsed = start.elapsed();
        println!(
            "Elapsed: {}",
            (elapsed.as_secs() as f64) + (f64::from(elapsed.subsec_nanos()) / 1_000_000_000.0)
        )
    }
}

/* 40% faster than lines(), 20% faster than read_line() */
fn create_gcs(in_filename: &str, out_filename: &str, fp: u64, index_gran: u64) -> io::Result<()> {
    let raw_infile = File::open(in_filename)?;
    let outfile = BufWriter::new(OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(out_filename)?);

    println!("Estimating lines");
    let n = estimate_lines(&raw_infile)?;
    println!("Estimate {} items", n);
    let mut infile = BufReader::new(raw_infile);

    println!("Estimated memory use: {} MB.", (n * 8) / (1024 * 1024));
    if n > 1024 * 1024 * 2 {
        println!("^C now and get a better computer if memory constrained");
        thread::sleep(time::Duration::from_millis(4000));
    }

    let mut count = 0;
    let start = Instant::now();
    let mut gcs = GCSBuilder::new(outfile, n, fp, index_gran).expect("Couldn't initialize builder");

    let mut line: Vec<u8> = Vec::with_capacity(128);
    while infile
        .by_ref()
        .take(128)
        .read_until(b'\n', &mut line)
        .unwrap_or(0) > 0
    {
        if let Some(hash) = u64_from_hex(&line[0..15]) {
            gcs.add(hash);

            count += 1;
            if count % 10_000_000_usize == 0 {
                println!(
                    " >> {} of {}, {:.1}% ({}/sec)",
                    count,
                    n,
                    (count as f64 / n as f64) * 100.0,
                    count
                        .checked_div(start.elapsed().as_secs() as usize)
                        .unwrap_or(0)
                );
            }
        } else {
            println!("Skipping line: {:?}", line);
        }
        line.clear();
    }

    println!("Writing out GCS");
    gcs.finish()?;
    println!("Done in {} seconds", start.elapsed().as_secs());

    Ok(())
}

fn main() {
    let args = clap_app!(gcstool =>
        (@setting SubcommandRequiredElseHelp)
        (version: "0.0.1")
        (author: "Thomas Hurst <tom@hir.st>")
        (about: "Golomb Compressed Sets tool -- compact set membership database.")
        (@arg verbose: -v --verbose "Be verbose")
        (@subcommand create =>
            (about: "Create GCS database from file")
            (@arg probability: -p +takes_value default_value("16777216") "False positive rate for queries, 1-in-p.")
            (@arg index_granularity: -i +takes_value default_value("1024") "Entries per index point (16 bytes each).")
            (@arg INPUT: +required "Input file")
            (@arg OUTPUT: +required "Database to build")
        )
        (@subcommand query =>
            (about: "Query a database")
            (@arg FILE: +required "Database to query")
        )
    ).get_matches();

    let stderr = &mut std::io::stderr();

    match args.subcommand() {
        ("create", Some(matches)) => {
            let in_filename = matches.value_of("INPUT").unwrap();
            let out_filename = matches.value_of("OUTPUT").unwrap();

            let fp = value_t!(matches, "probability", u64).unwrap_or_else(|e| e.exit());
            let index_gran =
                value_t!(matches, "index_granularity", u64).unwrap_or_else(|e| e.exit());

            if let Err(e) = create_gcs(in_filename, out_filename, fp, index_gran) {
                writeln!(stderr, "Error: {}", e).ok();

                std::process::exit(1);
            }
        }
        ("query", Some(matches)) => {
            let filename = matches.value_of("FILE").unwrap();

            let outfile = File::open(filename).expect("can't open input");
            query_gcs(outfile);
        }
        _ => { /* not reached */ }
    }
}
