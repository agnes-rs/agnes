# Agnes

[![Build Status](https://travis-ci.org/agnes-rs/agnes.svg?branch=master)](https://travis-ci.org/agnes-rs/agnes)
[![Documentation](https://docs.rs/agnes/badge.svg)](https://docs.rs/agnes)
[![Join the chat at https://gitter.im/agnes-rs/community](https://badges.gitter.im/agnes-rs/community.svg)](https://gitter.im/agnes-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

`agnes` is a data wrangling library for Rust.

Some useful links:
* [Guide](https://wee.codes/agnes/guide.html)
* [API Documentation](https://docs.rs/agnes)
* [Examples](examples)
* [Gitter chatroom](https://gitter.im/agnes-rs/community)

## Overview

`agnes` is a statically-typed high-performance data processing library for the Rust programming language. It provides utilities for data loading, preprocessing, aggregation, annotation, and serialization. The primary goal of `agnes` is to to provide functionality to help in the development of robust, efficient, readable applications for your data preprocessing tasks.

### Features

* Data structures for handling heterogeneously-typed tabular data.
* Extensible data source framework which currently supports the loading of local or web-based CSV files,
with more data source types under development.
* Data output through [serde](https://github.com/serde-rs/serde), allowing output to any serialization output `serde` supports.
* Handling of missing data (NaNs) for all data types.
* Data merging and joining to combine data from multiple sources.
* Iterator- or index-based data access

### Design Principles

`agnes` was designed with the following general principles in mind:

* Minimal data duplication: a single data source can be shared by multiple data views or outputs.
* Type safety -- `agnes` leverages Rust's typing system to provide the compile-time advantges
that static typing provides.
* Embracing of existing Rust paradigms and best practices. This includes the use of iterators,
explicit memory control, and existing Rust libraries (such as `serde`).

## Usage

To use, add this this to your `Cargo.toml`:

```toml
[dependencies]
agnes = "0.3"
```

and this to your crate root:

```rust
extern crate agnes;
```

## Example

As an simple example, let's build an application that reads in a data set, and displays it. A more complete example illustrating much more `agnes` functionality can be found in the guide [here](https://wee.codes/agnes/guide.html).

This example loads specific fields from a country-by-country data file, and shows off the table definition format, source specification format, and loading-from-URI functionality. This example can also be found [here](examples/gdp_uri.rs).

```rust
#[macro_use]
extern crate agnes;

use agnes::source::csv::load_csv_from_uri;

// specify the GDP table (only the fields we are concerned about)
tablespace![
    table gdp {
        CountryName: String,
        CountryCode: String,
        Gdp2015: f64,
    }
];

fn main() {
    // specify the source location for our GDP fields
    let gdp_spec = spec![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Gdp2015 = "2015";
    ];

    // load the CSV file from a URI
    let gdp_view =
        load_csv_from_uri("https://wee.codes/data/gdp.csv", gdp_spec).expect("CSV loading failed.");

    // print the DataView
    println!("{}", gdp_view);
}
```

## Changes and Future Plans

* [Future Work](FUTURE.md)
* [Changelog](https://github.com/jblondin/agnes/releases)

## License

This work is licensed under the [MIT Licence](LICENSE).
