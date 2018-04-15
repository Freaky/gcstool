use std::fs::{File, OpenOptions};
use std::io;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::time::Instant;
use std::{thread, time};

extern crate byteorder;
extern crate memchr;
extern crate rayon;
extern crate sha1;

#[macro_use]
extern crate clap;

use memchr::Memchr;

mod bitio;
mod gcs;
mod line_reader;
mod status;

use gcs::*;
use line_reader::*;
use status::Status;

const ESTIMATE_LIMIT: u64 = 1024 * 1024 * 16;

fn estimate_lines(mut inp: &std::fs::File) -> io::Result<u64> {
    let size = inp.metadata()?.len();
    let sample_size = std::cmp::min(size, ESTIMATE_LIMIT) as usize;

    let mut buffer: Vec<u8> = vec![0; sample_size];
    inp.read_exact(&mut buffer)?;
    inp.seek(SeekFrom::Start(0))?;

    let sample = Memchr::new(b'\n', &buffer).count() as u64;

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

fn query_gcs<P: AsRef<Path>>(filename: P) -> io::Result<()> {
    let file = File::open(filename)?;
    let file = BufReader::new(file);
    let mut searcher = GCSReader::new(file);
    searcher.initialize()?;

    let mut stdout = io::stdout();
    let stdin = io::stdin();

    println!(
        "Ready for queries on {} items with a 1 in {} false-positive rate.  ^D to exit.",
        searcher.n, searcher.p
    );
    print!("> ");
    stdout.flush()?;

    for line in stdin.lock().lines() {
        let line = line?;

        let hash = sha1::Sha1::from(&line).digest().to_string();
        let val = u64_from_hex(&hash.as_bytes()[0..16]).expect("Error in... SHA1. What.");

        let start = Instant::now();
        let exists = searcher.exists(val).expect("Error in search");
        let elapsed = start.elapsed();
        println!(
            "{} in {:.1}ms",
            if exists { "Found" } else { "Not found" },
            (elapsed.as_secs() as f64) * 1000.0 + (f64::from(elapsed.subsec_nanos()) / 1_000_000.0)
        );
        print!("> ");
        stdout.flush()?;
    }
    println!("Exit");

    Ok(())
}

fn create_gcs<P: AsRef<Path>>(
    in_filename: P,
    out_filename: P,
    fp: u64,
    index_gran: u64,
) -> io::Result<()> {
    let infile = File::open(in_filename)?;
    let outfile = BufWriter::with_capacity(
        1024 * 256,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(out_filename)?,
    );

    let n = estimate_lines(&infile)?;
    println!(
        "Estimated memory use for {} items: {} MB.",
        n,
        (n * 8) / (1024 * 1024)
    );
    if n > 1024 * 1024 * 1024 * 2 {
        println!("^C now and get a better computer if memory constrained");
        thread::sleep(time::Duration::from_millis(4000));
    }

    let mut status = Status::new(1);

    let mut gcs = GCSBuilder::new(outfile, n, fp, index_gran).expect("Couldn't initialize builder");

    // infile.lines(): 2.27 M/sec
    // infile.read_line(): 2.56 M/sec (by saving String allocation)
    // infile.read_until(): 2.85 M/sec (by avoiding UTF-8 processing)
    // infile.take(128).read_until(): 2.7 M/sec
    // LineReader::next_line(): 3.8 M/sec

    status.stage_work("Hashing", n);
    let mut reader = LineReader::new(infile);
    while let Some(line) = reader.next_line() {
        let line = line.unwrap();
        if let Some(hash) = u64_from_hex(&line[0..16]) {
            gcs.add(hash);

            status.incr();
        } else {
            println!("Skipping line: {:?}", line);
        }
    }

    gcs.finish(&mut status)?;
    status.done();

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
            let in_filename = matches.value_of_os("INPUT").unwrap();
            let out_filename = matches.value_of_os("OUTPUT").unwrap();

            let fp = value_t!(matches, "probability", u64).unwrap_or_else(|e| e.exit());
            let index_gran =
                value_t!(matches, "index_granularity", u64).unwrap_or_else(|e| e.exit());

            if let Err(e) = create_gcs(in_filename, out_filename, fp, index_gran) {
                writeln!(stderr, "Error: {}", e).ok();

                std::process::exit(1);
            }
        }
        ("query", Some(matches)) => {
            let filename = matches.value_of_os("FILE").unwrap();

            if let Err(e) = query_gcs(filename) {
                writeln!(stderr, "Error: {}", e).ok();

                std::process::exit(1);
            }
        }
        _ => {
            panic!("You're not supposed to get here.  Hi.");
        }
    }
}
