
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
	buf: [u8; 1],
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
			self.flush()?;
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
			self.write_bit(if (value & mask) > 0 { 1 } else { 0 })?;

			mask >>= 1
		}

		Ok(())
	}

	fn flush(&mut self) ->  io::Result<()> {
		if self.mask != 128 {
			self.io.write_all(&[self.byte])?;
			self.mask = 128;
			self.byte = 0;
		}

		Ok(())
	}
}

impl<T: io::Read> BitBufReader<T> {
	pub fn new(io: T) -> BitBufReader<T> {
		BitBufReader {
			io: io,
			buf: [0],
			mask: 0
		}
	}
}


impl<T: io::Read> BitReader for BitBufReader<T> {
	fn read_bit(&mut self) -> io::Result<u8> {
		if self.mask == 0 {
			self.io.read_exact(&mut self.buf)?;
			self.mask = 128;
		}

		let bit = if self.mask & self.buf[0] > 0 { 1 } else { 0 };

		if self.mask == 1 {   // MSB 0
			self.mask = 0;
		} else {
			self.mask >>= 1; // MSB 0
		}

		// println!(" << {}", bit);

		Ok(bit)
	}

	fn read_bits_u64(&mut self, nbits: u8) -> io::Result<u64> {
		assert!(nbits < 64);

		// println!("read_bits({})", nbits);

		let mut bits: u64 = 0;

		for i in 0..nbits {
			let bit = self.read_bit()? as u64;
			// println!("bit {}: {}", i, bit);
			// bits += (1 << i) * bit;  // LSB 0
			bits += (1 << (nbits - i - 1)) * bit; // MSB 0
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

	fn encode(&mut self, val: u64) -> io::Result<()> {
		let q:u64 = val / self.p;
		let r:u64 = val % self.p;

		self.out.write_bits((q + 1) as u8, ((1 << (q + 1)) - 2))?;
		self.out.write_bits(self.log2p, r)?;

		Ok(())
	}

	fn finish(&mut self) -> io::Result<()> {
		self.out.flush()
	}
}

struct GCSBuilder<T: BitWriter> {
	encoder: GolombEncoder<T>,
	n: u64,
	p: u64,
	values: Vec<u64>,
}

impl<T: BitWriter> GCSBuilder<T> {
	fn new(out: T, n: u64, p: u64) -> GCSBuilder<T> {
		GCSBuilder {
			encoder: GolombEncoder::new(out, p),
			n: n,
			p: p,
			values: Vec::with_capacity(n as usize),
		}
	}

	fn add(&mut self, data: std::string::String) {
		let h = u64::from_str_radix(&data[0..15], 16).unwrap() % (self.n * self.p);

		self.values.push(h);
	}

	fn finish(&mut self) -> io::Result<()> {
		self.values.sort_unstable();

		let mut diff: u64;
		let mut last: u64 = 0;
		for v in &self.values {
			diff = v - last;
			last = *v;

			if diff > 0 {
				self.encoder.encode(diff)?;
			}
		}

		self.encoder.finish()
	}
}

struct GolombDecoder<T> {
	reader: T,
	p: u64,
	log2p: u8
}

impl<T: BitReader> GolombDecoder<T> {
	fn new(reader: T, p: u64) -> GolombDecoder<T> {
		GolombDecoder::<T> {
			reader: reader,
			p: p,
			log2p: (p as f64).log2().ceil().trunc() as u8
		}
	}

	fn next(&mut self) -> io::Result<u64> {
		let mut v: u64 = 0;

		while self.reader.read_bit()? == 1 {
			v += self.p;
		}

		v += self.reader.read_bits_u64(self.log2p)?;
		Ok(v)
	}
}

struct GCSReader<T> {
	decoder: GolombDecoder<T>,
	last: u64
}

impl<T: BitReader> GCSReader<T> {
	fn new(reader: T, p: u64) -> GCSReader<T> {
		GCSReader {
			decoder: GolombDecoder::new(reader, p),
			last: 0
		}
	}

	fn next(&mut self) -> io::Result<u64> {
		self.last = self.last + self.decoder.next()?;
		Ok(self.last)
	}
}

use std::{thread, time};
use std::time::Instant;

const INPUT_BUFFER_SIZE: usize = 1024 * 1024;
const FALSE_POSITIVE_RATE: u64 = 10_000;

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
	let test_bitreader = BitBufReader::new(test_inbuf);
	let mut decoder = GCSReader::new(test_bitreader, fp);

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
			_ => break
		}
	}
}


fn build_gcs<R: io::Read + std::io::Seek, W: io::Write>(infile: R, outfile: W, fp: u64) {
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
	let bitwriter = BitBufWriter::new(buf_out);
	let mut gcs = GCSBuilder::new(bitwriter, n, fp);
	for line in buf_in.lines() {
		gcs.add(line.unwrap()); // .expect("Error adding item to GCS builder");

		count += 1;
		if count % 10_000_000 == 0 {
			println!(" >> {} of {}, {:.1}% ({}/sec)",
			         count, n, (count as f64 / n as f64) * 100.0, count / start.elapsed().as_secs());
		}
	}

	println!("Writing out GCS");
	gcs.finish().expect("Error writing GCS");
	println!("Done in {} seconds", start.elapsed().as_secs());
}

fn main() {
	let args: Vec<String> = env::args().collect();
	let fp = FALSE_POSITIVE_RATE;

	match args.len() {
		3 => {
			let in_filename = &args[1];
			let out_filename = &args[2];

			let infile = File::open(in_filename).expect("can't open input");
			let outfile = File::create(out_filename).expect("can't open output");

			build_gcs(infile, outfile, fp);
		},
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
