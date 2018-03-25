use std::mem;
use std::io::{self, Read, BufReader};
use std::fs::File;

use hyper;
use futures::{Future, Stream};
use futures::stream::StreamFuture;
use hyper::client::Client;

use hyper_tls::HttpsConnector;
use tokio_core::reactor::Core;

use source::FileLocator;
use error::*;

/// A file source. Abstracts over different kinds of file sources.
#[derive(Clone, Debug)]
pub struct FileSource {
    /// file location.
    pub file: FileLocator,
}

impl FileSource {
    /// Create a new file source from a file location.
    pub fn new<T: Into<FileLocator>>(src: T) -> FileSource {
        FileSource {
            file: src.into(),
        }
    }
    /// Callback-based reading from a file source. Calls the function `callback` with
    /// chunks of the data until finished. Synchronous; blocks execution.
    pub fn read_chunks<F>(&self, mut callback: F) -> Result<()> where F: FnMut(&[u8]) {
        match self.file {
            FileLocator::File(ref path) => {
                let file = File::open(path)?;
                const DEFAULT_BUF_SIZE: usize = 8 * 1024;
                let mut reader = BufReader::with_capacity(DEFAULT_BUF_SIZE, file);
                let mut buf = [0; DEFAULT_BUF_SIZE];
                loop {
                    let bytes_read = reader.read(&mut buf)?;
                    if bytes_read > 0 {
                        callback(&buf[0..bytes_read]);
                    } else {
                        break;
                    }
                }
            },
            FileLocator::Http(ref uri) => {
                let mut core = Core::new()?;
                let handle = core.handle();
                let client = Client::configure()
                    .connector(HttpsConnector::new(4, &handle)?)
                    .build(&handle);
                let work = client.get(uri.clone()).and_then(|res| {
                    res.body().for_each(|chunk| Ok(callback(&*chunk)))
                });
                core.run(work)?;
            }
        }
        Ok(())
    }
}

impl<T: Into<FileLocator>> From<T> for FileSource {
    fn from(orig: T) -> FileSource {
        FileSource::new(orig)
    }
}

/// Struct that implements `Read` for file sources.
#[derive(Debug)]
pub struct FileReader(Inner);

#[derive(Debug)]
enum Inner {
    File(File),
    Http {
        core: Core,
        response_state: State,
    },
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

impl FileReader {
    /// Create new reader from a file source.
    pub fn new(src: FileSource) -> Result<FileReader> {
        match src.file {
            FileLocator::File(ref path) => {
                let file = File::open(path)?;
                Ok(FileReader(Inner::File(file)))
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
                Ok(FileReader(Inner::Http { core: core, response_state: State::Awaiting(resp) }))
            }
        }
    }
}
impl Read for FileReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match self.0 {
            Inner::File(ref mut file) => {
                file.read(out)
            },
            Inner::Http { ref mut response_state, ref mut core } => {
                // Check the existing response state, temporarily storing the 'Empty' state so
                // we can move stuff out of the current state
                let (mut body, mut buf) = match mem::replace(response_state, State::Empty) {
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
    }
}
