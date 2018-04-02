
use std::io;

pub trait BitReader {
	fn read_bit(&mut self) -> io::Result<u8>;
	fn read_bits_u64(&mut self, nbits: u8) -> io::Result<u64>;
}

pub struct BitBufReader<T: io::Read> {
	io: T,
	buf: [u8; 1],
	mask: u8,
}

impl<T: io::Read> BitBufReader<T> {
	pub fn new(io: T) -> BitBufReader<T> {
		BitBufReader {
			io: io,
			buf: [0],
			mask: 0,
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

		if self.mask == 1 {
			// MSB 0
			self.mask = 0;
		} else {
			self.mask >>= 1;
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
			bits += (1 << (nbits - i - 1)) * bit;
		}

		Ok(bits)
	}
}



pub trait BitWriter {
	fn write_bit(&mut self, value: u8) -> io::Result<()>;
	fn write_bits(&mut self, nbits: u8, value: u64) -> io::Result<(usize)>;
	fn flush(&mut self) -> io::Result<()>;
}

pub struct BitBufWriter<T: io::Write> {
	io: T,
	byte: u8,
	mask: u8,
}

impl<T: io::Write> BitBufWriter<T> {
	pub fn new(io: T) -> BitBufWriter<T> {
		BitBufWriter {
			io: io,
			byte: 0,
			mask: 128,
		}
	}
}

impl<T: io::Write> BitWriter for BitBufWriter<T> {
	fn write_bit(&mut self, bit: u8) -> io::Result<()> {
		assert!(bit <= 1);

		self.byte += self.mask * bit;

		if self.mask == 1 {
			self.flush()?;
		} else {
			self.mask >>= 1;
		}

		Ok(())
	}

	fn write_bits(&mut self, nbits: u8, value: u64) -> io::Result<usize> {
		let mut mask: u64 = 1 << (nbits - 1);

		for _ in 0..nbits {
			self.write_bit(if (value & mask) > 0 { 1 } else { 0 })?;

			mask >>= 1
		}

		Ok(nbits as usize)
	}

	fn flush(&mut self) -> io::Result<()> {
		if self.mask != 128 {
			self.io.write_all(&[self.byte])?;
			self.mask = 128;
			self.byte = 0;
		}

		Ok(())
	}
}

