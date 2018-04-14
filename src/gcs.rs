use std::io;
use std::io::SeekFrom;
use std::io::{Error, ErrorKind};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rayon::prelude::*;

use bitio::{BitReader, BitWriter};
use status::Status;

const GCS_MAGIC: &[u8; 8] = b"[GCS:v0]";

pub struct GolombEncoder<W> {
    p: u64,
    log2p: u8,
    inner: BitWriter<W>,
}

impl<W: io::Write> GolombEncoder<W> {
    pub fn new(inner: W, p: u64) -> Self {
        Self {
            p,
            log2p: (p as f64).log2().ceil().trunc() as u8,
            inner: BitWriter::<W>::new(inner),
        }
    }

    pub fn encode(&mut self, val: u64) -> io::Result<usize> {
        let q: u64 = val / self.p;
        let r: u64 = val % self.p;

        let mut written = 0;

        written += self.inner.write_bits((q + 1) as u8, (1 << (q + 1)) - 2)?;
        written += self.inner.write_bits(self.log2p, r)?;

        Ok(written)
    }

    fn finish(&mut self) -> io::Result<usize> {
        self.inner.flush()
    }

    fn into_inner(self) -> W {
        self.inner.into_inner()
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
    pub fn new(
        io: T,
        n: u64,
        p: u64,
        index_granularity: u64,
    ) -> Result<GCSBuilder<T>, &'static str> {
        match n.checked_mul(p) {
            Some(_) => Ok(GCSBuilder {
                io,
                n,
                p,
                index_granularity: index_granularity as usize,
                values: Vec::with_capacity(n as usize),
            }),
            None => Err("n*p must fit in u64"),
        }
    }

    pub fn add(&mut self, value: u64) {
        self.values.push(value);
    }

    pub fn finish(mut self, status: &mut Status) -> io::Result<()> {
        self.n = self.values.len() as u64;
        let np = match self.n.checked_mul(self.p) {
            Some(np) => np,
            None => {
                return Err(Error::new(ErrorKind::Other, "n*p must fit in u64"));
            }
        };

        status.stage("Normalise");
        self.values.par_iter_mut().for_each(|v| *v %= np);

        status.stage("Sort");
        self.values.par_sort_unstable();

        status.stage("Deduplicate");
        self.values.dedup();

        let index_points = self.values.len() / self.index_granularity;

        // v => bit position
        let mut index: Vec<(u64, u64)> = Vec::with_capacity(index_points);
        let mut encoder = GolombEncoder::new(self.io, self.p);

        let mut diff: u64;
        let mut last: u64 = 0;
        let mut total_bits: u64 = 0;

        status.stage("Encode");

        for (i, v) in self.values.iter().enumerate() {
            diff = v - last;
            last = *v;

            let bits_written = encoder.encode(diff)?;

            total_bits += bits_written as u64;

            status.incr();

            if self.index_granularity > 0 && i > 0 && i % self.index_granularity == 0 {
                index.push((*v, total_bits));
            }
        }

        let end_of_data = total_bits + encoder.finish()? as u64;
        assert!(end_of_data % 8 == 0);

        let end_of_data = end_of_data / 8;

        self.io = encoder.into_inner();

        status.stage("Index");
        // Write the index: pairs of u64's (value, bit index)
        for &(v, pos) in &index {
            self.io.write_u64::<BigEndian>(v)?;
            self.io.write_u64::<BigEndian>(pos)?;
        }
        status.finish_stage();

        // Write our footer
        // N, P, index position in bytes, index size in entries [magic]
        // 5*8=40 bytes
        self.io.write_u64::<BigEndian>(self.n)?;
        self.io.write_u64::<BigEndian>(self.p)?;
        self.io.write_u64::<BigEndian>(end_of_data as u64)?;
        self.io.write_u64::<BigEndian>(index.len() as u64)?;
        self.io.write_all(GCS_MAGIC)?;
        self.io.flush()?;

        Ok(())
    }
}

pub struct GCSReader<R> {
    inner: BitReader<R>,
    pub n: u64,
    pub p: u64,
    end_of_data: u64,
    index_len: u64,
    index: Vec<(u64, u64)>,
    log2p: u8,
}

impl<R: io::Read + io::Seek> GCSReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: BitReader::new(inner),
            n: 0,
            p: 0,
            end_of_data: 0,
            index_len: 0,
            index: Vec::with_capacity(0),
            log2p: 0,
        }
    }

    pub fn initialize(&mut self) -> io::Result<()> {
        let io = self.inner.get_mut();
        io.seek(SeekFrom::End(-40))?;

        self.n = io.read_u64::<BigEndian>()?;
        self.p = io.read_u64::<BigEndian>()?;

        self.log2p = (self.p as f64).log2().ceil().trunc() as u8;

        self.end_of_data = io.read_u64::<BigEndian>()?;
        self.index_len = io.read_u64::<BigEndian>()?;

        let mut hdr = [0; 8];
        io.read_exact(&mut hdr)?;
        if hdr != *GCS_MAGIC {
            return Err(Error::new(ErrorKind::Other, "Not a GCS file"));
        }

        io.seek(SeekFrom::Start(self.end_of_data))?;

        // slurp in the index.
        self.index.reserve(1 + self.index_len as usize);
        self.index.push((0, 0)); // implied

        for _ in 0..self.index_len {
            self.index
                .push((io.read_u64::<BigEndian>()?, io.read_u64::<BigEndian>()?));
        }

        Ok(())
    }

    pub fn exists(&mut self, target: u64) -> io::Result<bool> {
        let h = target % (self.n * self.p);

        let entry = match self.index.binary_search_by_key(&h, |&(v, _p)| v) {
            Ok(_) => return Ok(true),
            Err(e) => self.index[e.saturating_sub(1)],
        };
        let mut last = entry.0;
        let bit_pos = entry.1;

        self.inner.seek(SeekFrom::Start(bit_pos))?;

        while last < h {
            while self.inner.read_bit()? == 1 {
                last += self.p;
            }

            last += self.inner.read_bits(self.log2p)?;
        }

        Ok(last == h)
    }
}
