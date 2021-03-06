use std;

use encoding::all::{ISO_8859_1, WINDOWS_1252};
use encoding::{DecoderTrap, Encoding};

use error::*;

#[inline]
pub(crate) fn decode(bytes: &[u8]) -> Result<String> {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .or_else(|_| {
            // fallback to ISO-8859-1 encoding
            ISO_8859_1.decode(bytes, DecoderTrap::Strict)
        })
        .or_else(|_| {
            // fallback to WINDOWS-1252 encoding
            WINDOWS_1252.decode(bytes, DecoderTrap::Strict)
        })
        .map_err(|_| AgnesError::Decode("unabled to decode input".to_string()))
}
