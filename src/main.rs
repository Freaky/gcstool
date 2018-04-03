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
extern crate sha1;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

mod bitio;
use bitio::*;

const INPUT_BUFFER_SIZE: usize = 1024 * 1024;
const FALSE_POSITIVE_RATE: u64 = 10_000_000;
const INDEX_GRANULARITY: u64 = 512;

const GCS_MAGIC: &[u8; 8] = b"[GCS:v0]";

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

	fn finish<T: io::Write>(&mut self, mut io: T) -> io::Result<usize> {
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
		let mut index: Vec<(u64, u64)> = Vec::with_capacity(index_points);
		let mut encoder = GolombEncoder::new(self.p);

		let mut diff: u64;
		let mut last: u64 = 0;
		let mut total_bits: u64 = 0;

		for (i, v) in self.values.iter().enumerate() {
			diff = v - last;
			last = *v;

			let bits_written = encoder.encode(&mut self.io, diff)?;

			total_bits += bits_written as u64;

			if self.index_granularity > 0 && i > 0 && i % self.index_granularity == 0 {
				index.push((*v, total_bits));
			}
		}

		println!("Total bits written: {}", total_bits);
		println!("Index entries: {} (expected {})", index.len(), index_points);
		assert!(index.len() == index_points);

		let end_of_data = total_bits + encoder.finish(&mut self.io)? as u64;
		println!("end of data = {}", end_of_data);
		assert!(end_of_data % 8 == 0);

		let end_of_data = end_of_data / 8;

		// Write the index: pairs of u64's (value, bit index)
		for &(v, pos) in index.iter() {
			self.io.write_u64::<BigEndian>(v)?;
			self.io.write_u64::<BigEndian>(pos)?;
		}

		// Write our footer
		// [delim] N, P, index position in bytes, index size in entries [delim]
		// 6*8=48 bytes
		assert!(GCS_MAGIC.len() == 8);
		self.io.write_all(GCS_MAGIC)?;
		self.io.write_u64::<BigEndian>(self.n)?;
		self.io.write_u64::<BigEndian>(self.p)?;
		self.io.write_u64::<BigEndian>(end_of_data as u64)?;
		self.io.write_u64::<BigEndian>(index.len() as u64)?;
		self.io.write_all(GCS_MAGIC)?;

		Ok(())
	}
}

struct GCSReader<T> {
	io: T,
	n: u64,
	p: u64,
	end_of_data: u64,
	index_len: u64,
	index: Vec<(u64, u64)>,
	last: u64,
	log2p: u8,
}

impl<T: io::Read + io::Seek> GCSReader<T> {
	fn new(io: T) -> GCSReader<T> {
		GCSReader {
			io: io,
			n: 0,
			p: 0,
			last: 0,
			end_of_data: 0,
			index_len: 0,
			index: Vec::with_capacity(0),
			log2p: 0,
		}
	}

	fn initialize(&mut self) -> io::Result<()> {
		self.io.seek(SeekFrom::End(-48))?;
		let mut hdr = [0; 8];
		self.io.read_exact(&mut hdr)?;
		assert!(hdr == *GCS_MAGIC);
		self.n = self.io.read_u64::<BigEndian>()?;
		self.p = self.io.read_u64::<BigEndian>()?;
		self.log2p = (self.p as f64).log2().ceil().trunc() as u8;
		self.end_of_data = self.io.read_u64::<BigEndian>()?;
		self.index_len = self.io.read_u64::<BigEndian>()?;
		let mut hdr = [0; 8];
		self.io.read_exact(&mut hdr)?;
		assert!(hdr == *GCS_MAGIC);

		self.io.seek(SeekFrom::Start(self.end_of_data));

		// slurp in the index.
		self.index.reserve(self.index_len as usize);

		for _ in 0..self.index_len {
			self.index.push((self.io.read_u64::<BigEndian>()?, self.io.read_u64::<BigEndian>()?));
		}

		Ok(())
	}

	fn exists(&mut self, data: &str) -> io::Result<bool> {
		let h = u64::from_str_radix(&data[0..15], 16).unwrap() % (self.n * self.p);

		let nearest = match self.index.binary_search_by_key(&h, |&(v,p)| v) {
			Ok(i) => { return Ok(true) },
			Err(i) => { if i == 0 { i } else { i - 1 } }
		};

		let bit_pos = self.index[nearest].1;
		let bit_offset = (bit_pos % 8) as u8;
		let byte_offset = bit_pos / 8;

		self.io.seek(SeekFrom::Start(byte_offset))?;
		let mut reader = BitReader::new();
		if bit_pos > 0 {
			reader.read_bits_u64(&mut self.io, bit_offset)?;
		}

		let mut last = self.index[nearest].0;
		while last < h {
			let mut v: u64 = 0;

			while reader.read_bit(&mut self.io)? == 1 {
				v += self.p;
			}

			v += reader.read_bits_u64(&mut self.io, self.log2p)?;
			last = v + last;

			// println!("next: {}", last);
		}

		if last == h {
			return Ok(true);
		} else {
			return Ok(false);
		}
	}
}

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
		println!("Elapsed: {} ms",
             (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64)
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
			test(outfile);
		}
		_ => {
			println!("Usage: {} infile outfile", args[0]);
			std::process::exit(1);
		}
	}
}
