#[macro_use]
extern crate agnes;
extern crate csv_sniffer;
extern crate typenum;

use std::fmt::Debug;
use std::path::Path;

use agnes::field::Value;
use agnes::join::{Equal, Join};
use agnes::source::csv::{CsvReader, CsvSource, IntoCsvSrcSpec};

fn load_csv_file<Spec>(filename: &str, spec: Spec) -> CsvReader<Spec::CsvSrcSpec>
where
    Spec: IntoCsvSrcSpec,
    <Spec as IntoCsvSrcSpec>::CsvSrcSpec: Debug,
{
    let data_filepath = Path::new(file!()) // start as this file
        .parent()
        .unwrap() // navigate up to examples directory
        .join("data") // navigate into data directory
        .join(filename); // navigate to target file

    let source = CsvSource::new(data_filepath.into()).unwrap();
    CsvReader::new(&source, spec).unwrap()
}

namespace![
    pub namespace gdp {
        field CountryName: String;
        field CountryCode: String;
        field Year1983: f64;
    }
];

namespace![
    pub namespace gdp_metadata: gdp {
        field CountryCode: String;
        field Region: String;
    }
];

namespace![
    pub namespace life: gdp_metadata {
        field CountryCode: String;
        field Year1983: f64;
    }
];

namespace![
    pub namespace gdp_life: life {
        field Gdp1983: f64;
        field Life1983: f64;
    }
];

fn main() {
    let gdp_spec = spec![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Year1983 = "1983";
    ];

    let mut csv_rdr = load_csv_file("gdp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv", gdp_spec);
    let dv_gdp = csv_rdr.read().unwrap().into_view().v::<Labels![
        gdp::CountryName,
        gdp::CountryCode,
        gdp::Year1983
    ]>();

    let gdp_metadata_spec = spec![
        fieldindex gdp_metadata::CountryCode = 0usize;
        fieldname gdp_metadata::Region = "Region";
    ];

    let mut csv_rdr = load_csv_file(
        "gdp/Metadata_Country_API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv",
        gdp_metadata_spec,
    );
    let mut dv_gdp_metadata = csv_rdr
        .read()
        .unwrap()
        .into_view()
        .v::<Labels![gdp_metadata::CountryCode, gdp_metadata::Region]>();

    dv_gdp_metadata.filter::<gdp_metadata::Region, _>(|val: Value<&String>| val.exists());

    let dv_gdp_joined = dv_gdp
        .join::<Join<gdp::CountryCode, gdp_metadata::CountryCode, Equal>, _, _>(&dv_gdp_metadata);

    let life_spec = spec![
        fieldname life::CountryCode = "Country Code";
        fieldname life::Year1983 = "1983";
    ];
    let mut csv_rdr = load_csv_file("life/API_SP.DYN.LE00.IN_DS2_en_csv_v2.csv", life_spec);
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
