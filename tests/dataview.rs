#[macro_use]
extern crate agnes;
extern crate csv_sniffer;
extern crate serde;
extern crate serde_json;
extern crate typenum;

mod common;

namespace![
    pub namespace gdp {
        field CountryName: String;
        field CountryCode: String;
        field Year1983: f64;
    }
];

#[test]
fn subview() {
    use gdp::*;

    let gdp_spec = spec![
        fieldname CountryName = "Country Name";
        fieldname CountryCode = "Country Code";
        fieldname Year1983 = "1983";
    ];

    let (mut csv_rdr, _) = common::load_csv_file("gdp.csv", gdp_spec);
    let dv = csv_rdr.read().unwrap().into_view();
    assert_eq!(dv.nfields(), 3);
    let subdv = dv.v::<Labels![CountryName, Year1983]>();
    assert_eq!(subdv.nrows(), 264);
    assert_eq!(subdv.nfields(), 2);
}

namespace![
    pub namespace sample {
        field State: String;
        field Value1: u64;
        field Value2: f64;
    }
];

namespace![
    pub namespace sample2 {
        field ST: String;
    }
];

#[test]
fn rename() {
    use sample::*;
    use sample2::*;

    let sample_spec = spec![
        fieldname State = "state";
        fieldname Value1 = "val1";
        fieldname Value2 = "val2";
    ];
    let (mut csv_rdr, _) = common::load_csv_file("sample1.csv", sample_spec);

    let dv = csv_rdr.read().unwrap().into_view();
    println!("{}", dv);
    // println!("{}", serde_json::to_string(&dv).unwrap());
    let dv = dv.relabel::<State, ST>();
    println!("{}", dv);
    // println!("{}", serde_json::to_string(&dv).unwrap());
}

namespace![
    pub namespace life: gdp {
        field CountryName: String;
        field CountryCode: String;
        field Year1983: f64;
    }
];

namespace![
    pub namespace renamed: life {
        field Gdp1983: f64;
        field Life1983: f64;
    }
];

#[test]
fn merge() {
    let gdp_spec = spec![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Year1983 = "1983";
    ];
    let (mut csv_rdr, _) = common::load_csv_file("gdp.csv", gdp_spec);
    let dv_gdp = csv_rdr.read().unwrap().into_view().v::<Labels![
        gdp::CountryName,
        gdp::CountryCode,
        gdp::Year1983
    ]>();

    let life_spec = spec![
        fieldname life::CountryName = "Country Name";
        fieldname life::CountryCode = "Country Code";
        fieldname life::Year1983 = "1983";
    ];
    let (mut csv_rdr, _) = common::load_csv_file("life.csv", life_spec);
    let dv_life = csv_rdr.read().unwrap().into_view();
    // only take extra '1983' column
    let dv_life = dv_life.v::<Labels![life::Year1983]>();

    let dv_gdp = dv_gdp.relabel::<gdp::Year1983, renamed::Gdp1983>();
    let dv_life = dv_life.relabel::<life::Year1983, renamed::Life1983>();

    let dv = dv_gdp.merge(&dv_life).unwrap();
    println!("{}", dv);
    assert_eq!(
        dv.fieldnames(),
        vec!["CountryName", "CountryCode", "Gdp1983", "Life1983"]
    );
}
