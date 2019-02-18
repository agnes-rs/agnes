#[macro_use]
extern crate agnes;

use agnes::source::csv::load_csv_from_uri;

tablespace![
    table gdp {
        CountryName: String,
        CountryCode: String,
        Gdp2015: f64,
    }
];

fn main() {
    let gdp_spec = spec![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Gdp2015 = "2015";
    ];

    // load the CSV file from a URI
    let gdp_view =
        load_csv_from_uri("https://wee.codes/data/gdp.csv", gdp_spec).expect("CSV loading failed.");

    println!("{}", gdp_view);
}
