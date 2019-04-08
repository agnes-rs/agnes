#[macro_use]
extern crate agnes;

use agnes::access::DataIndex;
use agnes::field::Value;
use agnes::join::{Equal, Join};
use agnes::label::IntoLabeled;
use agnes::select::FieldSelect;
use agnes::source::csv::load_csv_from_uri;
use agnes::store::IntoView;

tablespace![
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
    pub table transformed {
        GdpEUR2015: f64,
    }
];

fn usd_to_eur(usd: &f64) -> f64 {
    usd * 0.88395
}

fn main() {
    let gdp_schema = schema![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Gdp2015 = "2015";
    ];

    // load the GDP CSV file from a URI
    let gdp_view = load_csv_from_uri("https://wee.codes/data/gdp.csv", gdp_schema)
        .expect("CSV loading failed.");

    let gdp_metadata_schema = schema![
        fieldindex gdp_metadata::CountryCode = 0usize;
        fieldname gdp_metadata::Region = "Region";
    ];

    // load the metadata CSV file from a URI
    let gdp_metadata_view = load_csv_from_uri(
        "https://wee.codes/data/gdp_metadata.csv",
        gdp_metadata_schema,
    )
    .expect("CSV loading failed.");

    let gdp_metadata_view =
        gdp_metadata_view.filter::<gdp_metadata::Region, _>(|val: Value<&String>| val.exists());

    let gdp_country_view = gdp_view
        .join::<Join<gdp::CountryCode, gdp_metadata::CountryCode, Equal>, _, _>(&gdp_metadata_view);

    let life_schema = schema![
        fieldname life::CountryCode = "Country Code";
        fieldname life::Life2015 = "2015";
    ];

    // load the life expectancy file from a URI
    let life_view = load_csv_from_uri("https://wee.codes/data/life.csv", life_schema)
        .expect("CSV loading failed.");

    let gdp_life_view =
        gdp_country_view.join::<Join<gdp::CountryCode, life::CountryCode, Equal>, _, _>(&life_view);

    let gdp_life_view =
        gdp_life_view.v::<Labels![gdp::CountryName, gdp::Gdp2015, life::Life2015]>();

    let transformed_gdp = gdp_life_view
        .field::<gdp::Gdp2015>()
        .iter()
        .map_existing(usd_to_eur)
        .label::<transformed::GdpEUR2015>()
        .into_view();

    let final_view = gdp_life_view
        .merge(&transformed_gdp)
        .expect("Merge failed.")
        .v::<Labels![gdp::CountryName, transformed::GdpEUR2015, life::Life2015]>();

    println!("{}", final_view);
}
