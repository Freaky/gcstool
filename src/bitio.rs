use std::io;

const MASKS: [u64; 65] = [
    0,
    0b1,
    0b11,
    0b111,
    0b1111,
    0b11111,
    0b111111,
    0b1111111,
    0b11111111,
    0b111111111,
    0b1111111111,
    0b11111111111,
    0b111111111111,
    0b1111111111111,
    0b11111111111111,
    0b111111111111111,
    0b1111111111111111,
    0b11111111111111111,
    0b111111111111111111,
    0b1111111111111111111,
    0b11111111111111111111,
    0b111111111111111111111,
    0b1111111111111111111111,
    0b11111111111111111111111,
    0b111111111111111111111111,
    0b1111111111111111111111111,
    0b11111111111111111111111111,
    0b111111111111111111111111111,
    0b1111111111111111111111111111,
    0b11111111111111111111111111111,
    0b111111111111111111111111111111,
    0b1111111111111111111111111111111,
    0b11111111111111111111111111111111,
    0b111111111111111111111111111111111,
    0b1111111111111111111111111111111111,
    0b11111111111111111111111111111111111,
    0b111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111111111111111111111,
    0b11111111111111111111111111111111111111111111111111111111111111,
    0b111111111111111111111111111111111111111111111111111111111111111,
    0b1111111111111111111111111111111111111111111111111111111111111111,
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
        assert!(nbits <= 64);

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
