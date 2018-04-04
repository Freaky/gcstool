use std::env;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader,BufWriter};
use std::fs::{OpenOptions,File};
use std::{thread, time};
use std::time::Instant;

extern crate bytecount;
extern crate byteorder;
extern crate sha1;

mod bitio;
mod gcs;

use gcs::*;

const INPUT_BUFFER_SIZE: usize = 1024 * 1024;
const FALSE_POSITIVE_RATE: u64 = 1000;
const INDEX_GRANULARITY: u64 = 512;

fn count_lines<R: BufRead + std::io::Seek>(mut inp: R) -> io::Result<u64> {
	let mut buffer: Vec<u8> = vec![0; INPUT_BUFFER_SIZE];
	let mut n: u64 = 0;
	loop {
		let len = inp.read(&mut buffer[0..INPUT_BUFFER_SIZE])?;
		if len == 0 {
			break;
		}

		n += bytecount::count(&buffer, b'\n') as u64;
	}

	inp.seek(SeekFrom::Start(0))?;

	Ok(n)
}

fn test<R: io::Read + io::Seek>(test_in: R) {
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
		let start = Instant::now();
		println!("Search: {:?}", searcher.exists(&hash));
		let elapsed = start.elapsed();
		println!("Elapsed: {}", (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0))
	}
}

fn build_gcs<R: io::Read + std::io::Seek, W: io::Write>(infile: R, outfile: W, fp: u64, index_granularity: u64) -> io::Result<()> {
	let mut buf_in = BufReader::new(infile);

	println!("Counting items");

	let n = count_lines(&mut buf_in)?;

	println!("Counted {} items", n);

	println!("Approx memory use: {} MB.", (n * 8) / (1024 * 1024));
	if n > 1000 * 1000 {
		println!("^C now and get a better computer if memory constrained");
		thread::sleep(time::Duration::from_millis(4000));
	}

	println!("Building Golomb Compressed Set");

	let buf_out = BufWriter::new(outfile);

	let start = Instant::now();

	let mut count = 0;
	let mut gcs = GCSBuilder::new(buf_out, n, fp, index_granularity).unwrap();
	for line in buf_in.lines() {
		gcs.add(&line.unwrap());

		count += 1;
		if count % 10_000_000 == 0 {
			println!(
				" >> {} of {}, {:.1}% ({}/sec)",
				count,
				n,
				(count as f64 / n as f64) * 100.0,
				count / start.elapsed().as_secs()
			);
		}
	}

	println!("Writing out GCS");
	gcs.finish()?;
	println!("Done in {} seconds", start.elapsed().as_secs());
	Ok(())
}

fn build_gcs_with_filenames(in_filename: &str, out_filename: &str, fp: u64, index_gran: u64) -> io::Result<()> {
	let infile = File::open(in_filename)?;
	let outfile = OpenOptions::new().write(true).create_new(true).open(out_filename)?;

	build_gcs(infile, outfile, fp, index_gran)?;

	Ok(())
}

fn main() {
	let args: Vec<String> = env::args().collect();
	let fp = FALSE_POSITIVE_RATE;
	let index_gran = INDEX_GRANULARITY;

	let stderr = &mut std::io::stderr();

	match args.len() {
		3 => {
			let in_filename = &args[1];
			let out_filename = &args[2];

			if let Err(e) = build_gcs_with_filenames(in_filename, out_filename, fp, index_gran) {
				writeln!(stderr, "Error: {}", e).ok();

				std::process::exit(1);
			}
		}
		2 => {
			let filename = &args[1];

			let outfile = File::open(filename).expect("can't open input");
			test(outfile);
		}
		_ => {
			println!("Usage: {} infile outfile", args[0]);
			std::process::exit(1);
		}
	}
}
