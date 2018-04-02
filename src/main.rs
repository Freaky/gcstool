use std::env;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader,BufWriter};
use std::fs::File;
use std::{thread, time};
use std::time::Instant;

extern crate bytecount;
extern crate byteorder;

use byteorder::{BigEndian, WriteBytesExt};

mod bitio;
use bitio::*;

struct GolombEncoder {
	writer: BitWriter,
	p: u64,
	log2p: u8,
}

impl GolombEncoder {
	fn new(p: u64) -> GolombEncoder {
		GolombEncoder {
			writer: BitWriter::new(),
			p: p,
			log2p: (p as f64).log2().ceil().trunc() as u8,
		}
	}

	fn encode<T: io::Write>(&mut self, mut io: T, val: u64) -> io::Result<usize> {
		let q: u64 = val / self.p;
		let r: u64 = val % self.p;

		let mut written = 0;

		written += self.writer.write_bits(&mut io, (q + 1) as u8, ((1 << (q + 1)) - 2))?;
		written += self.writer.write_bits(&mut io, self.log2p, r)?;

		Ok(written)
	}

	fn finish<T: io::Write>(&mut self, mut io: T) -> io::Result<()> {
		self.writer.flush(&mut io)
	}
}

struct GCSBuilder<T: io::Write> {
	io: T,
	n: u64,
	p: u64,
	index_granularity: usize,
	values: Vec<u64>,
}

impl<T: io::Write> GCSBuilder<T> {
	fn new(out: T, n: u64, p: u64, index_granularity: u64) -> Result<GCSBuilder<T>, &'static str> {
		match n.checked_mul(p) {
			Some(_) => Ok(GCSBuilder {
				io: out,
				n: n,
				p: p,
				index_granularity: index_granularity as usize,
				values: Vec::with_capacity(n as usize),
			}),
			None => Err("n*p must fit in u64")
		}
	}

	fn add(&mut self, data: &str) {
		let h = u64::from_str_radix(&data[0..15], 16).unwrap() % (self.n * self.p);

		self.values.push(h);
	}

	fn finish(&mut self) -> io::Result<()> {
		self.values.sort_unstable();
		self.values.dedup();

		let index_points = self.values.len() / self.index_granularity;

		// v => bit position
		let mut index: Vec<(usize, usize)> = Vec::with_capacity(index_points);
		let mut encoder = GolombEncoder::new(self.p);

		let mut diff: u64;
		let mut last: u64 = 0;
		let mut total_bits: usize = 0;

		for (i, v) in self.values.iter().enumerate() {
			diff = v - last;
			last = *v;

			let bits_written = encoder.encode(&mut self.io, diff)?;

			if self.index_granularity > 0 && i > 0 && i % self.index_granularity == 0 {
				index.push((i, total_bits));
			}

			total_bits += bits_written;
		}

		println!("Total bits written: {}", total_bits);
		println!("Index entries: {} (expected {})", index.len(), index_points);

		encoder.finish(&mut self.io)?;

		// let mut io = BufWriter::new(File::create("test.index").unwrap());

		for (v, pos) in index {
			self.io.write_u64::<BigEndian>(v as u64)?;
			self.io.write_u64::<BigEndian>(pos as u64)?;
		}

		// Write our footer
		self.io.write_u64::<BigEndian>(total_bits as u64)?;
		self.io.write(b"FREAKY:GCS:1")?;

		Ok(())
	}
}

struct GolombDecoder {
	reader: BitReader,
	p: u64,
	log2p: u8,
}

impl GolombDecoder {
	fn new(p: u64) -> GolombDecoder {
		GolombDecoder {
			reader: BitReader::new(),
			p: p,
			log2p: (p as f64).log2().ceil().trunc() as u8,
		}
	}

	fn next<T: io::Read>(&mut self, mut io: T) -> io::Result<u64> {
		let mut v: u64 = 0;

		while self.reader.read_bit(&mut io)? == 1 {
			v += self.p;
		}

		v += self.reader.read_bits_u64(&mut io, self.log2p)?;
		Ok(v)
	}
}

struct GCSReader<T> {
	io: T,
	decoder: GolombDecoder,
	last: u64,
}

impl<T: io::Read> GCSReader<T> {
	fn new(io: T, p: u64) -> GCSReader<T> {
		GCSReader {
			io: io,
			decoder: GolombDecoder::new(p),
			last: 0,
		}
	}

	fn next(&mut self) -> io::Result<u64> {
		self.last = self.last + self.decoder.next(&mut self.io)?;
		Ok(self.last)
	}
}

const INPUT_BUFFER_SIZE: usize = 1024 * 1024;
const FALSE_POSITIVE_RATE: u64 = 10_000_000;
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

fn test<R: io::Read>(test_in: R, fp: u64) {
	let test_inbuf = BufReader::new(test_in);
	let mut decoder = GCSReader::new(test_inbuf, fp);

	let mut last = 0;
	loop {
		let v = decoder.next();
		match v {
			Ok(v) => {
				if last >= v {
					println!("Dodgy value: {} => {}", last, v);
				}
				last = v;
				// println!(" << {}", v),
			}
			_ => break,
		}
	}
}

fn build_gcs<R: io::Read + std::io::Seek, W: io::Write>(infile: R, outfile: W, fp: u64, index_granularity: u64) {
	let mut buf_in = BufReader::new(infile);

	println!("Counting items");

	let n = count_lines(&mut buf_in).unwrap();

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
	// let bitwriter = BitBufWriter::new(buf_out);
	let mut gcs = GCSBuilder::new(buf_out, n, fp, index_granularity).unwrap();
	for line in buf_in.lines() {
		gcs.add(&line.unwrap()); // .expect("Error adding item to GCS builder");

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
	gcs.finish().expect("Error writing GCS");
	println!("Done in {} seconds", start.elapsed().as_secs());
}

fn main() {
	let args: Vec<String> = env::args().collect();
	let fp = FALSE_POSITIVE_RATE;
	let index_gran = INDEX_GRANULARITY;

	match args.len() {
		3 => {
			let in_filename = &args[1];
			let out_filename = &args[2];

			let infile = File::open(in_filename).expect("can't open input");
			let outfile = File::create(out_filename).expect("can't open output");

			build_gcs(infile, outfile, fp, index_gran);
		}
		2 => {
			let filename = &args[1];

			let outfile = File::open(filename).expect("can't open input");
			test(outfile, fp);
		}
		_ => {
			println!("Usage: {} infile outfile", args[0]);
			std::process::exit(1);
		}
	}
}
