
use std::env;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::fs::File;

pub trait BitWriter {
	fn write_bits(&mut self, nbits: u8, value: u64) -> io::Result<()>;
	fn flush(&mut self) -> io::Result<()>;
}

pub struct BitBuffer<T: io::Write> {
	out: T,
	byte: u8,
	mask: u8,
}

impl<T: io::Write> BitBuffer<T> {
	pub fn new(out: T) -> BitBuffer<T> {
		BitBuffer {
			out: out,
			byte: 0,
			mask: 0x08
		}
	}
}

impl<T: io::Write> BitWriter for BitBuffer<T> {
	fn write_bits(&mut self, nbits: u8, value: u64) -> io::Result<()> {
		let mut mask = 1 << (nbits as u64 - 1);
		while mask > 0 {
			// println!("mask: {}, self mask: {}", mask, self.mask);
			if value & mask > 0 {
				self.byte |= self.mask;
			}

			self.mask >>= 1;
			mask >>= 1;

			if self.mask == 0 {
				try!(self.flush());
			}
		}

		Ok(())
	}

	fn flush(&mut self) ->  io::Result<()> {
		if self.mask == 0x80 {
			return Ok(());
		}

		self.mask = 0x80;
		try!(self.out.write_all(&[self.byte]));
		self.byte = 0;

		Ok(())
	}
}

struct GolombEncoder<T: BitWriter> {
	out: T,
	p: u64,
	n: u64,
	log2p: u8
}

impl<T: BitWriter> GolombEncoder<T> {
	fn new(out: T, n: u64, p: u64) -> GolombEncoder<T> {
		GolombEncoder {
			out: out,
			n: n,
			p: p,
			log2p: (p as f64).log2().ceil().trunc() as u8
		}
	}

	fn encode(&mut self, val: u64) {
		let q = val / self.p;
		let r = val % self.p;

		self.out.write_bits((q + 1) as u8, ((1 << (q + 1)) - 2));
		self.out.write_bits(self.log2p, r);
	}

	fn finish(&mut self) {
		self.out.flush();
	}
}

pub struct GCSBuilder {
	encoder: GolombEncoder,
	n: u64,
	p: u64,
	values: Vec<u64>,
}

impl GCSBuilder {
	fn new(out: io::Write, n: u64, p: u64) -> GCSBuilder {
		GCSBuilder {
			encoder: GolombEncoder::new(out, n, p),
			n: n,
			p: p,
			values: vec![]
		}
	}

	fn add(&mut self, data: &[u8]) {
		let h = u64::from_str_radix(data, 16).unwrap() % (self.n * self.p);

		self.values.push(h);
	}

	fn finalise(&mut self) {
		self.values.sort_unstable();

		let mut diff: u64 = 0;
		let mut last: u64 = 0;
		for v in self.values {
			diff = v - last;
			last = v;

			if diff > 0 {
				self.encoder.encode(diff);
			}
		}

		self.encoder.finish();
	}
}
/*
fn gcs_hash(data, n, p) -> u64 {
	u64::from_str_radix(data, 16).unwrap() % (n * p);
}*/

use std::io::SeekFrom;

extern crate bytecount;

const INPUT_BUFFER_SIZE: usize = 1024 * 1024;

fn main() {
	let args: Vec<String> = env::args().collect();

	if args.len() < 3 {
		println!("Usage: {} infile outfile", args[0]);
		std::process::exit(1);
	}
	let in_filename = &args[1];
	let out_filename = &args[2];

	let mut infile = File::open(in_filename).expect("can't open input");
	let mut outfile = File::create(out_filename).expect("can't open output");

	let mut inmeta = infile.metadata;

	let mut buf_in = BufReader::new(infile);
	let mut buf_out = BufWriter::new(outfile);

	let n_bytes = infile.metadata().unwrap().len() as usize;

	println!("Counting items in {}", in_filename);

	let mut buffer: Vec<u8> = vec![0; INPUT_BUFFER_SIZE];
	let mut n: usize = 0;
	loop {
		let len = buf_in.read(&mut buffer[0..INPUT_BUFFER_SIZE]).expect("read error");
		if len == 0 {
			break;
		}

		n += bytecount::count(&buffer, b'\n');
	}
	println!("Counted {} items", n);
	println!("Building Golomb Compressed Set in {}", out_filename);

	buf_in.seek(SeekFrom::Start(0)).expect("seek error");

	let fp = 50_000_000;

	let mut gcs = GCSBuilder::new(&buf_out, n, fp);
	for line in buf_in.lines() {

	}
}
