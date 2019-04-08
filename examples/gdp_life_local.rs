#[macro_use]
extern crate agnes;

use std::fmt::Debug;
use std::path::Path;

use agnes::field::Value;
use agnes::join::{Equal, Join};
use agnes::source::csv::{CsvReader, CsvSource, IntoCsvSrcSchema};

fn load_csv_file<Schema>(filename: &str, schema: Schema) -> CsvReader<Schema::CsvSrcSchema>
where
    Schema: IntoCsvSrcSchema,
    <Schema as IntoCsvSrcSchema>::CsvSrcSchema: Debug,
{
    let data_filepath = Path::new(file!()) // start as this file
        .parent()
        .unwrap() // navigate up to examples directory
        .join("data") // navigate into data directory
        .join(filename); // navigate to target file

    let source = CsvSource::new(data_filepath).unwrap();
    CsvReader::new(&source, schema).unwrap()
}

tablespace![
    pub table gdp {
        CountryName: String,
        CountryCode: String,
        Year1983: f64,
    }
    pub table gdp_metadata {
        CountryCode: String,
        Region: String,
    }
    pub table life {
        CountryCode: String,
        Year1983: f64,
    }
    pub table gdp_life {
        Gdp1983: f64,
        Life1983: f64,
    }
];

fn main() {
    let gdp_schema = schema![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Year1983 = "1983";
    ];

    let mut csv_rdr = load_csv_file("gdp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv", gdp_schema);
    let dv_gdp = csv_rdr.read().unwrap().into_view().v::<Labels![
        gdp::CountryName,
        gdp::CountryCode,
        gdp::Year1983
    ]>();

    let gdp_metadata_schema = schema![
        fieldindex gdp_metadata::CountryCode = 0usize;
        fieldname gdp_metadata::Region = "Region";
    ];

    let mut csv_rdr = load_csv_file(
        "gdp/Metadata_Country_API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv",
        gdp_metadata_schema,
    );
    let dv_gdp_metadata = csv_rdr
        .read()
        .unwrap()
        .into_view()
        .v::<Labels![gdp_metadata::CountryCode, gdp_metadata::Region]>();

    let dv_gdp_metadata =
        dv_gdp_metadata.filter::<gdp_metadata::Region, _>(|val: Value<&String>| val.exists());

    let dv_gdp_joined = dv_gdp
        .join::<Join<gdp::CountryCode, gdp_metadata::CountryCode, Equal>, _, _>(&dv_gdp_metadata);

    let life_schema = schema![
        fieldname life::CountryCode = "Country Code";
        fieldname life::Year1983 = "1983";
    ];
    let mut csv_rdr = load_csv_file("life/API_SP.DYN.LE00.IN_DS2_en_csv_v2.csv", life_schema);
    let dv_life = csv_rdr
        .read()
        .unwrap()
        .into_view()
        .v::<Labels![life::CountryCode, life::Year1983]>();

    let dv_gdp_joined = dv_gdp_joined.relabel::<gdp::Year1983, gdp_life::Gdp1983>();
    let dv_life = dv_life.relabel::<life::Year1983, gdp_life::Life1983>();

    let dv = dv_gdp_joined
        .join::<Join<gdp::CountryCode, life::CountryCode, Equal>, _, _>(&dv_life)
        .v::<Labels![gdp::CountryName, gdp_life::Gdp1983, gdp_life::Life1983]>();

    println!("{}", dv);
}
