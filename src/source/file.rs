use std::mem;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::File;

use hyper;
use futures::Stream;
use futures::stream::StreamFuture;
use hyper::client::Client;
use tempfile;

use hyper_tls::HttpsConnector;
use tokio_core::reactor::Core;

use source::FileLocator;
use error::*;

#[derive(Debug)]
pub struct LocalFileReader {
    file: File
}
impl LocalFileReader {
    /// Create new reader from a file locator, creating a temporary locl file if the file specified
    /// by the locator is non-local.
    pub fn new(loc: &FileLocator) -> Result<LocalFileReader> {
        match *loc {
            FileLocator::File(ref path) => {
                let file = File::open(path)?;
                Ok(LocalFileReader { file: file })
            },
            FileLocator::Http(_) => {
                // download file up to nbytes and save it to temp directory
                const BUF_SIZE: usize = 1 << 13; // 8 * 1024
                let mut buffer = vec![0; BUF_SIZE];
                let mut file_reader = HttpFileReader::new(loc)?;
                //TODO: change this to tempfile_in(..) to allow for configurable temp directory
                let mut temp_file: File = tempfile::tempfile()?;
                loop {
                    let n_read = file_reader.read(&mut buffer)?;
                    if n_read == 0 {
                        break;
                    }
                    let n_wrote = temp_file.write(&buffer[0..n_read])?;
                    if n_read != n_wrote {
                        return Err(io::Error::new(io::ErrorKind::WriteZero,
                            "unable to write to temporary file").into());
                    }
                }
                temp_file.seek(SeekFrom::Start(0))?;
                Ok(LocalFileReader { file: temp_file })
            }
        }
    }
}
impl Read for LocalFileReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.file.read(out)
    }
}
impl Seek for LocalFileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

#[derive(Debug)]
pub struct HttpFileReader {
    core: Core,
    response_state: State,
}
impl HttpFileReader {
    pub fn new(loc: &FileLocator) -> Result<HttpFileReader> {
        match *loc {
            FileLocator::File(_) => {
                return Err(NetError::LocalFile.into());
            },
            FileLocator::Http(ref uri) => {
                // establish event loop
                let mut core = Core::new()?;
                let handle = core.handle();
                // configure a HTTP client to retrieve the file
                let client = Client::configure()
                    .connector(HttpsConnector::new(4, &handle)?)
                    .build(&handle);
                // set up a future to retrieve the file.
                let resp = client.get(uri.clone());
                Ok(HttpFileReader { core: core, response_state: State::Awaiting(resp) })
            }
        }
    }
}
impl Read for HttpFileReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let (response_state, core) = (&mut self.response_state, &mut self.core);

        // Check the existing response state, temporarily storing the 'Empty' state so
        // we can move stuff out of the current state
        let (body, mut buf) = match mem::replace(response_state, State::Empty) {
            State::Awaiting(resp) => {
                // run the response future and block until we get it
                let resp = core.run(resp)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                (resp.body().into_future(), vec![])
            },
            State::Body { body, buffer } => (body, buffer),
            State::Empty => panic!("double empty!")
        };

        // start by putting everything from the buffer into the output
        let buflen = buf.len();
        let outlen = out.len();
        if buflen > 0 && buflen > outlen {
            // we have a buffer, but it's most the than available space in the output.
            // copy everything we can into the output, then remove that stuff from the
            // buffer
            out[..].copy_from_slice(&buf[0..outlen]);
            let tmp = buf.split_off(outlen);
            mem::replace(&mut buf, tmp);
            // Buffer is full, so we can go ahead and update the state and then return
            mem::replace(response_state, State::Body { body, buffer: buf });
            return Ok(outlen);
        }

        if buflen > 0 {
            // we have a buffer, and it's less than the output length (or we would've
            // already returned), so copy the whole buffer into the output.
            out[0..buflen].copy_from_slice(&buf[..]);
            buf.clear();
            // mem::replace(response_state, State::Body { body, buffer: buf });
        }

        // let's get the next chunk of the body
        let (chunk, body) = core.run(body)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.0))?;

        let total_len = match chunk {
            Some(ref chunk) => {
                let chunklen = chunk.len();
                if buflen + chunklen <= outlen {
                    // Chunk is smaller than output buffer, just copy everything over
                    // into output
                    out[buflen..buflen + chunklen].copy_from_slice(&chunk[..]);
                    buflen + chunklen
                } else {
                    // Chunk is larger than output buffer, copy what we can into the
                    // output, then put everythig else in a buffer
                    out[buflen..].copy_from_slice(&chunk[0..outlen - buflen]);
                    buf.extend_from_slice(&chunk[outlen - buflen..]);
                    outlen
                }
            },
            None => buflen
        };

        if total_len > 0 {
            mem::replace(response_state, State::Body {
                body: body.into_future(),
                buffer: buf
            });
        } else {
            mem::replace(response_state, State::Empty);
        }
        Ok(total_len)
    }
}

#[derive(Debug)]
enum State {
    Awaiting(hyper::client::FutureResponse),
    Body {
        body: StreamFuture<hyper::Body>,
        buffer: Vec<u8>
    },
    Empty
}

/// Abstract general file reader, implementing `Read`.
#[derive(Debug)]
pub enum FileReader {
    /// Implements `Read` for local files
    Local(LocalFileReader),
    /// Implements `Read` for http-served files
    Http(HttpFileReader),
}

impl FileReader {
    /// Create new reader from a file locator.
    pub fn new(loc: &FileLocator) -> Result<FileReader> {
        match *loc {
            FileLocator::File(_) => {
                Ok(FileReader::Local(LocalFileReader::new(loc)?))
            },
            FileLocator::Http(_) => {
                Ok(FileReader::Http(HttpFileReader::new(loc)?))
            }
        }
    }
}
impl Read for FileReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match *self {
            FileReader::Local(ref mut reader) => {
                reader.read(out)
            },
            FileReader::Http(ref mut reader) => {
                reader.read(out)
            }
        }
    }
}
