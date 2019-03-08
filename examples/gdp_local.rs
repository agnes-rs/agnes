#[macro_use]
extern crate agnes;

use agnes::source::csv::load_csv_from_path;

tablespace![
    table gdp {
        CountryName: String,
        CountryCode: String,
        Gdp2015: f64,
    }
];

fn main() {
    let gdp_schema = schema![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Gdp2015 = "2015";
    ];

    // load the CSV file from a path
    let gdp_view = load_csv_from_path(
        "./examples/data/gdp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv",
        gdp_schema,
    )
    .expect("CSV loading failed.");

    println!("{}", gdp_view);
}
