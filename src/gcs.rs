
use std::io;
use std::io::SeekFrom;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use bitio::*;

const GCS_MAGIC: &[u8; 8] = b"[GCS:v0]";

pub struct GolombEncoder {
	p: u64,
	log2p: u8,
	writer: BitWriter,
}

impl GolombEncoder {
	pub fn new(p: u64) -> GolombEncoder {
		GolombEncoder {
			p,
			log2p: (p as f64).log2().ceil().trunc() as u8,
			writer: BitWriter::new(),
		}
	}

	pub fn encode<T: io::Write>(&mut self, mut io: T, val: u64) -> io::Result<usize> {
		let q: u64 = val / self.p;
		let r: u64 = val % self.p;

		let mut written = 0;

		written += self.writer.write_bits(&mut io, (q + 1) as u8, (1 << (q + 1)) - 2)?;
		written += self.writer.write_bits(&mut io, self.log2p, r)?;

		Ok(written)
	}

	fn finish<T: io::Write>(&mut self, mut io: T) -> io::Result<usize> {
		self.writer.flush(&mut io)
	}
}

pub struct GCSBuilder<T: io::Write> {
	io: T,
	n: u64,
	p: u64,
	index_granularity: usize,
	values: Vec<u64>,
}

impl<T: io::Write> GCSBuilder<T> {
	pub fn new(io: T, n: u64, p: u64, index_granularity: u64) -> Result<GCSBuilder<T>, &'static str> {
		match n.checked_mul(p) {
			Some(_) => Ok(GCSBuilder {
				io,
				n,
				p,
				index_granularity: index_granularity as usize,
				values: Vec::with_capacity(n as usize),
			}),
			None => Err("n*p must fit in u64")
		}
	}

	pub fn add(&mut self, value: u64) {
		let h = value % (self.n * self.p);

		self.values.push(h);
	}

	pub fn finish(&mut self) -> io::Result<()> {
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
		for &(v, pos) in &index {
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

pub struct GCSReader<T> {
	io: T,
	n: u64,
	p: u64,
	end_of_data: u64,
	index_len: u64,
	index: Vec<(u64, u64)>,
	log2p: u8,
}

impl<T: io::Read + io::Seek> GCSReader<T> {
	pub fn new(io: T) -> GCSReader<T> {
		GCSReader {
			io,
			n: 0,
			p: 0,
			end_of_data: 0,
			index_len: 0,
			index: Vec::with_capacity(0),
			log2p: 0,
		}
	}

	pub fn initialize(&mut self) -> io::Result<()> {
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

		self.io.seek(SeekFrom::Start(self.end_of_data))?;

		// slurp in the index.
		self.index.reserve(self.index_len as usize);

		for _ in 0..self.index_len {
			self.index.push((self.io.read_u64::<BigEndian>()?, self.io.read_u64::<BigEndian>()?));
		}

		println!("Initialised GCS. n={}, p={}, index={}", self.n, self.p, self.index_len);

		Ok(())
	}

	pub fn exists(&mut self, target: u64) -> io::Result<bool> {
		let h = target % (self.n * self.p);

		let nearest = match self.index.binary_search_by_key(&h, |&(v,_p)| v) {
			Ok(_)  => { return Ok(true) },
			Err(e) => { e.saturating_sub(1) }
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
			while reader.read_bit(&mut self.io)? == 1 {
				last += self.p;
			}

			last += reader.read_bits_u64(&mut self.io, self.log2p)?;
		}

		Ok(last == h)
	}
}
