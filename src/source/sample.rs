use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::File;
use tempfile;

use source::file_locator::FileLocator;
use source::file::FileReader;
use error::*;

#[derive(Debug)]
pub struct SampleReader {
    file: File,
    nbytes: usize,
    pos: usize,
}

impl SampleReader {
    pub fn new(loc: &FileLocator, nbytes: usize) -> Result<SampleReader> {
        match *loc {
            FileLocator::File(ref path) => {
                let file = File::open(path)?;
                Ok(SampleReader {
                    file: file,
                    nbytes: nbytes,
                    pos: 0,
                })
            },
            FileLocator::Http(_) => {

                // download file up to nbytes and save it to temp directory
                const BUF_SIZE: usize = 1 << 13; // 8 * 1024
                let mut buffer = vec![0; BUF_SIZE.min(nbytes)];
                let mut file_reader = FileReader::new(loc)?;
                //TODO: change this to tempfile_in(..) to allow for configurable temp directory
                let mut temp_file: File = tempfile::tempfile()?;
                let mut ntotal = 0;

                while ntotal < nbytes {
                    let n_read = file_reader.read(&mut buffer)?;
                    if n_read == 0 {
                        break;
                    }
                    ntotal += n_read;
                    let n_wrote = temp_file.write(&buffer[0..n_read])?;
                    if n_read != n_wrote {
                        return Err(io::Error::new(io::ErrorKind::WriteZero,
                            "unable to write to temporary file").into());
                    }
                }

                Ok(SampleReader {
                    file: temp_file,
                    nbytes: nbytes,
                    pos: 0,
                })
            }
        }
    }

    pub fn into_file(self) -> File {
        self.file
    }
}

impl Read for SampleReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.nbytes {
            Ok(0)
        } else if self.pos + out.len() > self.nbytes {
            // we're going past sample size, read a subset
            let n = self.file.read(&mut out[..self.nbytes - self.pos])?;
            self.pos += n;
            Ok(n)
        } else {
            // we're within sample
            let n = self.file.read(out)?;
            self.pos += n;
            Ok(n)
        }
    }
}

impl Seek for SampleReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(_) => {
                self.pos = (self.file.seek(pos)? as usize).min(self.nbytes);
            },
            SeekFrom::End(i) => {
                if i < self.nbytes as i64 {
                    self.pos = self.file.seek(
                        SeekFrom::Start((self.nbytes as i64 - i) as u64))? as usize;
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput,
                           "invalid seek to a negative or overflowing position"));
                }
            },
            SeekFrom::Current(_) => {
                self.pos = (self.file.seek(pos)? as usize).min(self.nbytes);
            }
        }
        Ok(self.pos as u64)
    }
}
