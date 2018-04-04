
use std::io;

pub struct BitReader {
	buf: [u8; 1],
	mask: u8,
}

impl BitReader {
	pub fn new() -> BitReader {
		BitReader {
			buf: [0],
			mask: 0,
		}
	}

	#[allow(dead_code)]
	pub fn reset(&mut self) {
		self.buf[0] = 0;
		self.mask = 0;
	}

	pub fn read_bit<T: io::Read>(&mut self, mut io: T) -> io::Result<u8> {
		if self.mask == 0 {
			io.read_exact(&mut self.buf)?;
			self.mask = 128;
		}

		let bit = if self.mask & self.buf[0] > 0 { 1 } else { 0 };

		self.mask >>= 1;

		Ok(bit)
	}

	pub fn read_bits_u64<T: io::Read>(&mut self, mut io: T, nbits: u8) -> io::Result<u64> {
		assert!(nbits < 64);

		let mut bits: u64 = 0;

		for i in 0..nbits {
			let bit = self.read_bit(&mut io)? as u64;
			bits += (1 << (nbits - i - 1)) * bit;
		}

		Ok(bits)
	}
}

pub struct BitWriter {
	byte: u8,
	mask: u8,
}

impl BitWriter {
	pub fn new() -> BitWriter {
		BitWriter {
			byte: 0,
			mask: 128,
		}
	}

	pub fn reset(&mut self) {
		self.byte = 0;
		self.mask = 128;
	}

	pub fn write_bit<T: io::Write>(&mut self, mut io: T, bit: u8) -> io::Result<()> {
		assert!(bit <= 1);

		self.byte += self.mask * bit;

		if self.mask == 1 {
			self.flush(&mut io)?;
		} else {
			self.mask >>= 1;
		}

		Ok(())
	}

	pub fn write_bits<T: io::Write>(&mut self, mut io: T, nbits: u8, value: u64) -> io::Result<usize> {
		let mut mask: u64 = 1 << (nbits - 1);

		for _ in 0..nbits {
			self.write_bit(&mut io, if (value & mask) > 0 { 1 } else { 0 })?;

			mask >>= 1
		}

		Ok(nbits as usize)
	}

	// Return the number of padding bits
	pub fn flush<T: io::Write>(&mut self, mut io: T) -> io::Result<usize> {
		let bits_written: usize = match self.mask {
			128 => { return Ok(0) }
			64 => 7,
			32 => 6,
			16 => 5,
			8 => 4,
			4 => 3,
			2 => 2,
			1 => 1,
			_ => { panic!("invalid mask") }
		};
		io.write_all(&[self.byte])?;
		self.reset();
		Ok(bits_written)
	}
}

