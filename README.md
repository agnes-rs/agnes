# Agnes

[![Build Status](https://travis-ci.org/jblondin/agnes.svg?branch=master)](https://travis-ci.org/jblondin/agnes)
[![Documentation](https://docs.rs/agnes/badge.svg)](https://docs.rs/agnes)

Agnes is a data wrangling crate for Rust. It is intended to provide utilities for data loading, aggregation, annotation, and visualization.

It is still very much a work in progress.

# Setup

Add this to your `Cargo.toml`:

```toml
[dependencies]
agnes = "0.1"
```

and this to your crate root:

```rust
extern crate agnes;
```

# Example

A quick example:

```rust
extern crate csv_sniffer;

use std::env;

fn main() {
    let data_filepath = Path::new(file!()).parent().unwrap().join("tests/data/sample1.csv");
    let file = FileSource::new(data_filepath);
    let mut csv_rdr = CsvReader::new(
        CsvSourceBuilder::new(file)
            .fields(fields![
                "state" => FieldType::Text,
                "val1" => FieldType::Unsigned,
                "val2" => FieldType::Float
            ])
            .build()
    ).unwrap();
    let dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", dv);
}
```
