
use std::env;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::BufReader;
use std::io::BufWriter;
use std::fs::File;

extern crate bytecount;

trait BitWriter {
	fn write_bit(&mut self, value: u8) -> io::Result<()>;
	fn write_bits(&mut self, nbits: u8, value: u64) -> io::Result<()>;
	fn flush(&mut self) -> io::Result<()>;
}

trait BitReader {
	fn read_bit(&mut self) -> io::Result<u8>;
	fn read_bits_u64(&mut self, nbits: u8) -> io::Result<u64>;
}

struct BitBufReader<T: io::Read> {
	io: T,
	byte: u8,
	mask: u8
}

struct BitBufWriter<T: io::Write> {
	io: T,
	byte: u8,
	mask: u8,
}

impl<T: io::Write> BitBufWriter<T> {
	pub fn new(io: T) -> BitBufWriter<T> {
		BitBufWriter {
			io: io,
			byte: 0,
			mask: 128
		}
	}
}

impl<T: io::Write> BitWriter for BitBufWriter<T> {
	fn write_bit(&mut self, bit: u8) -> io::Result<()> {
		assert!(bit <= 1);
		// println!(" >> {}", bit);

		self.byte += self.mask * bit;

		if self.mask == 1 {
			try!(self.flush());
		} else {
			self.mask >>= 1;
		}

		Ok(())
	}

	fn write_bits(&mut self, nbits: u8, value: u64) -> io::Result<()> {
		let mut mask:u64 = 1 << (nbits - 1);
		// let mut mask = 1;
		// println!("write {} bits of: {:b}", nbits, value);

		for _ in 0..nbits {
			try!(self.write_bit(if (value & mask) > 0 { 1 } else { 0 }));

			mask >>= 1
		}

		Ok(())
	}

	fn flush(&mut self) ->  io::Result<()> {
		if self.mask != 128 {
			try!(self.io.write_all(&[self.byte]));
			self.mask = 128;
			self.byte = 0;
		}

		Ok(())
	}
}

impl<T: io::Read> BitReader for BitBufReader<T> {
	fn read_bit(&mut self) -> io::Result<u8> {
		// println!(" >> {}", bit);

		if self.mask == 128 {
			try!(self.io.read_exact(&mut [self.byte]));
			self.mask = 1;
		}

		let bit = if self.mask & self.byte > 0 { 1 } else { 0 };

		self.mask <<= 1;

		Ok(bit)
	}

	fn read_bits_u64(&mut self, nbits: u8) -> io::Result<u64> {
		assert!(nbits < 64);

		let mut bits: u64 = 0;

		for _ in 0..nbits {
			bits &= try!(self.read_bit()) as u64;
			bits <<= 1;
		}

		Ok(bits)
	}
}

struct GolombEncoder<T> {
	out: T,
	p: u64,
	log2p: u8
}

impl<T: BitWriter> GolombEncoder<T> {
	fn new(out: T, p: u64) -> GolombEncoder<T> {
		GolombEncoder::<T> {
			out: out,
			p: p,
			log2p: (p as f64).log2().ceil().trunc() as u8
		}
	}

	fn encode(&mut self, val: u64) {
		let q:u64 = val / self.p;
		let r:u64 = val % self.p;

		self.out.write_bits((q + 1) as u8, ((1 << (q + 1)) - 2)).expect("write failed");
		self.out.write_bits(self.log2p, r).expect("write failed");
	}

	fn finish(&mut self) {
		self.out.flush().expect("flush failed");
	}
}

struct GCSBuilder<T: BitWriter> {
	encoder: GolombEncoder<T>,
	n: u64,
	p: u64,
	values: Vec<u64>,
	last: u64,
}

impl<T: BitWriter> GCSBuilder<T> {
	fn new(out: T, n: u64, p: u64) -> GCSBuilder<T> {
		GCSBuilder {
			encoder: GolombEncoder::new(out, p),
			n: n,
			p: p,
			values: vec![0; n as usize],
			last: 0
		}
	}

/*
	fn add_sorted(&mut self, data: std::string::String) {
		let h: u64 = u64::from_str_radix(&data[0..15], 16).unwrap() % (self.n * self.p);

		assert!(self.last <= h);

		let diff = h - self.last;
		self.last = h;

		if diff > 0 {
			self.encoder.encode(diff);
		}
	}
*/

	fn add(&mut self, data: std::string::String) {
		let h = u64::from_str_radix(&data[0..15], 16).unwrap() % (self.n * self.p);

		self.values.push(h);
	}

	fn finish(&mut self) {
		self.values.sort_unstable();

		let mut diff: u64;
		let mut last: u64 = 0;
		for v in &self.values {
			diff = v - last;
			last = *v;

			if diff > 0 {
				self.encoder.encode(diff);
			}
		}

		self.encoder.finish();
	}
}

use std::{thread, time};

const INPUT_BUFFER_SIZE: usize = 1024 * 1024;
const FALSE_POSITIVE_RATE: u64 = 50_000_000;

fn count_lines<R: BufRead + std::io::Seek>(mut inp: R) -> u64 {
	let mut buffer: Vec<u8> = vec![0; INPUT_BUFFER_SIZE];
	let mut n: u64 = 0;
	loop {
		let len = inp.read(&mut buffer[0..INPUT_BUFFER_SIZE]).expect("read error");
		if len == 0 {
			break;
		}

		n += bytecount::count(&buffer, b'\n') as u64;
	}

	inp.seek(SeekFrom::Start(0)).expect("seek error");
	n
}

/*
fn is_sorted_file<R: BufRead + std::io::Seek>(mut inp: R) -> bool {
	let mut sorted = true;
	let mut last_num:u64 = 0;

	{
		let lines = inp.by_ref().lines().take(10000).take_while(Result::is_ok).map(Result::unwrap);
		for line in lines {
			let num = u64::from_str_radix(&line[0..15], 16).unwrap();
			if last_num > num {
				sorted = false;
				break;
			}
			last_num = num;
		}
	}

	inp.seek(SeekFrom::Start(0)).expect("seek error");
	return sorted;
}
*/

fn main() {
	let args: Vec<String> = env::args().collect();

	if args.len() < 3 {
		println!("Usage: {} infile outfile", args[0]);
		std::process::exit(1);
	}
	let in_filename = &args[1];
	let out_filename = &args[2];

	let mut infile = File::open(in_filename).expect("can't open input");
	let outfile = File::create(out_filename).expect("can't open output");
	let mut buf_in = BufReader::new(infile);

	println!("Counting items in {}", in_filename);

	let n = count_lines(&mut buf_in);

	println!("Counted {} items", n);

	println!("Approx memory use: {} MB.", (n * 8) / (1024 * 1024));
	if n > 1000 * 1000 {
		println!("^C now and get a better computer if memory constrained");
		thread::sleep(time::Duration::from_millis(4000));
	}

	println!("Building Golomb Compressed Set in {}", out_filename);

	let buf_out = BufWriter::new(outfile);

	buf_in.seek(SeekFrom::Start(0)).expect("seek error");

	let fp = FALSE_POSITIVE_RATE;

	let mut count = 0;
	let bitwriter = BitBufWriter::new(buf_out);
	let mut gcs = GCSBuilder::new(bitwriter, n, fp);
	for line in buf_in.lines() {
		gcs.add(line.unwrap());

		count += 1;
		if count % 10_000_000 == 0 {
			println!(" >> {} of {}, {:.1}%", count, n, (count as f64 / n as f64) * 100.0);
		}
	}
	gcs.finish();

/*
	let test_in = File::open(out_filename).expect("can't open database");
	let test_inbuf = BufReader::new(test_in);
	let test_bitreader = BitBufReader::new(test_inbuf);
	*/
}
