
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

struct BitBuffer<T: io::Write> {
	out: T,
	byte: u8,
	mask: u8,
}

impl<T: io::Write> BitBuffer<T> {
	pub fn new(out: T) -> BitBuffer<T> {
		BitBuffer {
			out: out,
			byte: 0,
			mask: 128
		}
	}
}

impl<T: io::Write> BitWriter for BitBuffer<T> {
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
		if self.mask == 128 {
			return Ok(());
		}

		try!(self.out.write_all(&[self.byte]));
		self.mask = 128;
		self.byte = 0;

		Ok(())
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

	fn add_sorted(&mut self, data: std::string::String) {
		let h = u64::from_str_radix(&data[0..15], 16).unwrap() % (self.n * self.p);

		assert!(self.last <= h);

		let diff = h - self.last;
		self.last = h;

		if diff > 0 {
			self.encoder.encode(diff);
		}
	}

	fn add(&mut self, data: std::string::String) {
		let h = u64::from_str_radix(&data[0..15], 16).unwrap() % (self.n * self.p);

		self.values.push(h);
	}

	fn finish(&mut self) {
		self.values.sort_unstable();

		let mut diff: u64;
		let mut last: u64 = 0;
		for v in self.values.iter() {
			diff = v - last;
			last = *v;

			if diff > 0 {
				self.encoder.encode(diff);
			}
		}

		self.encoder.finish();
	}
}

use std::thread;

const INPUT_BUFFER_SIZE: usize = 1024 * 1024;
const FALSE_POSITIVE_RATE: u64 = 50_000_000;

fn main() {
	let args: Vec<String> = env::args().collect();

	if args.len() < 3 {
		println!("Usage: {} infile outfile", args[0]);
		std::process::exit(1);
	}
	let in_filename = &args[1];
	let out_filename = &args[2];

	let infile = File::open(in_filename).expect("can't open input");
	let outfile = File::create(out_filename).expect("can't open output");
	let mut buf_in = BufReader::new(infile);
	let buf_out = BufWriter::new(outfile);

	println!("Counting items in {}", in_filename);

	let mut buffer: Vec<u8> = vec![0; INPUT_BUFFER_SIZE];
	let mut n: u64 = 0;
	loop {
		let len = buf_in.read(&mut buffer[0..INPUT_BUFFER_SIZE]).expect("read error");
		if len == 0 {
			break;
		}

		n += bytecount::count(&buffer, b'\n') as u64;
	}
	println!("Counted {} items", n);

	println!("Approx memory use: {} MB.", (n * 8) / (1024 * 1024));
	if n > 1000 * 1000 {
		println!("^C now and rebuild using sorted mode if memory is limited.");
		thread::sleep_ms(2000);
	}

	println!("Building Golomb Compressed Set in {}", out_filename);

	buf_in.seek(SeekFrom::Start(0)).expect("seek error");

	let fp = FALSE_POSITIVE_RATE;

	let mut count = 0;
	let bitwriter = BitBuffer::new(buf_out);
	let mut gcs = GCSBuilder::new(bitwriter, n, fp);
	for line in buf_in.lines() {
		gcs.add(line.unwrap());
		count += 1;
		if count % 10_000_000 == 0 {
			println!(" >> {} of {}, {:.1}%", count, n, (count as f64 / n as f64) * 100.0);
		}
	}
	gcs.finish();
}
