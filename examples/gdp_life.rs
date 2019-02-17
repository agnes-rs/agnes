#[macro_use]
extern crate agnes;

use agnes::field::Value;
use agnes::join::{Equal, Join};
use agnes::source::csv::load_csv_from_uri;

namespace![
    table gdp {
        CountryName: String,
        CountryCode: String,
        Gdp2015: f64,
    }
    table gdp_metadata {
        CountryCode: String,
        Region: String,
    }
    pub table life {
        CountryCode: String,
        Life2015: f64,
    }
];

fn main() {
    let gdp_spec = spec![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Gdp2015 = "2015";
    ];

    // load the GDP CSV file from a URI
    let gdp_view =
        load_csv_from_uri("https://wee.codes/data/gdp.csv", gdp_spec).expect("CSV loading failed.");

    let gdp_metadata_spec = spec![
        fieldindex gdp_metadata::CountryCode = 0usize;
        fieldname gdp_metadata::Region = "Region";
    ];

    // load the metadata CSV file from a URI
    let mut gdp_metadata_view =
        load_csv_from_uri("https://wee.codes/data/gdp_metadata.csv", gdp_metadata_spec)
            .expect("CSV loading failed.");

    gdp_metadata_view.filter::<gdp_metadata::Region, _>(|val: Value<&String>| val.exists());

    let gdp_country_view = gdp_view
        .join::<Join<gdp::CountryCode, gdp_metadata::CountryCode, Equal>, _, _>(&gdp_metadata_view);

    let life_spec = spec![
        fieldname life::CountryCode = "Country Code";
        fieldname life::Life2015 = "2015";
    ];

    let life_view = load_csv_from_uri("https://wee.codes/data/life.csv", life_spec)
        .expect("CSV loading failed.");

    let gdp_life_view =
        gdp_country_view.join::<Join<gdp::CountryCode, life::CountryCode, Equal>, _, _>(&life_view);

    let gdp_life_view =
        gdp_life_view.v::<Labels![gdp::CountryName, gdp::Gdp2015, life::Life2015]>();

    println!("{}", gdp_life_view);
}
