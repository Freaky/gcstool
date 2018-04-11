use std::io;

const MASKS: [u64; 65] = [
    0x0,
    0x1,
    0x3,
    0x7,
    0xf,
    0x1f,
    0x3f,
    0x7f,
    0xff,
    0x1ff,
    0x3ff,
    0x7ff,
    0xfff,
    0x1fff,
    0x3fff,
    0x7fff,
    0xffff,
    0x1ffff,
    0x3ffff,
    0x7ffff,
    0xfffff,
    0x1fffff,
    0x3fffff,
    0x7fffff,
    0xffffff,
    0x1ffffff,
    0x3ffffff,
    0x7ffffff,
    0xfffffff,
    0x1fffffff,
    0x3fffffff,
    0x7fffffff,
    0xffffffff,
    0x1ffffffff,
    0x3ffffffff,
    0x7ffffffff,
    0xfffffffff,
    0x1fffffffff,
    0x3fffffffff,
    0x7fffffffff,
    0xffffffffff,
    0x1ffffffffff,
    0x3ffffffffff,
    0x7ffffffffff,
    0xfffffffffff,
    0x1fffffffffff,
    0x3fffffffffff,
    0x7fffffffffff,
    0xffffffffffff,
    0x1ffffffffffff,
    0x3ffffffffffff,
    0x7ffffffffffff,
    0xfffffffffffff,
    0x1fffffffffffff,
    0x3fffffffffffff,
    0x7fffffffffffff,
    0xffffffffffffff,
    0x1ffffffffffffff,
    0x3ffffffffffffff,
    0x7ffffffffffffff,
    0xfffffffffffffff,
    0x1fffffffffffffff,
    0x3fffffffffffffff,
    0x7fffffffffffffff,
    0xffffffffffffffff,
];

pub struct BitReader {
    buffer: [u8; 1],
    unused: u8,
}

impl BitReader {
    pub fn new() -> Self {
        Self {
            buffer: [0],
            unused: 0,
        }
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.buffer[0] = 0;
        self.unused = 0;
    }

    pub fn read_bit<T: io::Read>(&mut self, mut io: T) -> io::Result<u8> {
        let bit = self.read_bits_u64(&mut io, 1)?;
        Ok(bit as u8)
    }

    pub fn read_bits_u64<T: io::Read>(&mut self, mut io: T, nbits: u8) -> io::Result<u64> {
        assert!(nbits < 64);

        let mut ret: u64 = 0;
        let mut rbits = nbits;

        while rbits > self.unused {
            ret |= (self.buffer[0] as u64) << (rbits - self.unused);
            rbits -= self.unused;

            io.read_exact(&mut self.buffer)?;

            self.unused = 8;
        }

        if rbits > 0 {
            ret |= (self.buffer[0] as u64) >> (self.unused - rbits);
            self.buffer[0] &= MASKS[(self.unused - rbits) as usize] as u8;
            self.unused -= rbits;
        }

        Ok(ret)
    }
}

pub struct BitWriter {
    buffer: u64,
    unused: u64,
}

impl BitWriter {
    pub fn new() -> Self {
        Self {
            buffer: 0,
            unused: 8,
        }
    }

    #[allow(dead_code)]
    pub fn write_bit<T: io::Write>(&mut self, mut io: T, bit: u8) -> io::Result<()> {
        assert!(bit <= 1);
        self.write_bits(&mut io, 1, bit as u64)?;
        Ok(())
    }

    pub fn write_bits<T: io::Write>(
        &mut self,
        mut io: T,
        nbits: u8,
        value: u64,
    ) -> io::Result<usize> {
        assert!(nbits <= 64);

        let mut nbits_remaining = nbits as u64;
        let mut value = value & MASKS[nbits as usize];

        while nbits_remaining >= self.unused {
            self.buffer = (self.buffer << self.unused) | (value >> (nbits_remaining - self.unused));

            // write low byte
            io.write_all(&[self.buffer as u8])?;

            nbits_remaining -= self.unused;
            value &= MASKS[nbits_remaining as usize];
            self.unused = 8;
            self.buffer = 0;
        }

        if nbits_remaining > 0 {
            self.buffer = (self.buffer << nbits_remaining) | value;
            self.unused -= nbits_remaining;
        }
        Ok(nbits as usize)
    }

    pub fn flush<T: io::Write>(&mut self, mut io: T) -> io::Result<usize> {
        if self.unused != 8 {
            io.write_all(&[(self.buffer << self.unused) as u8])?;
            let written = self.unused;
            self.unused = 8;
            Ok(written as usize)
        } else {
            Ok(0)
        }
    }
}
