extern crate agnes;
extern crate serde;
extern crate serde_json;

mod common;

use agnes::view::DataView;

#[test]
fn rename() {
    let mut csv_rdr = common::load_test_file("sample1.csv");

    let mut dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());
    dv.rename("state", "ST").unwrap();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());
}

#[test]
fn aggregate() {
    let mut csv_rdr = common::load_test_file("gdp.csv");
    let mut dv_gdp: DataView = DataView::from(csv_rdr.read().unwrap())
        .v(["Country Name", "Country Code", "1983"]);

    let mut csv_rdr = common::load_test_file("life.csv");
    // only take extra '1983' column
    let mut dv_life: DataView = DataView::from(csv_rdr.read().unwrap()).v("1983");


    dv_gdp.rename("1983", "1983 GDP").unwrap();
    dv_life.rename("1983", "1983 Life Expectancy").unwrap();

    let dv = dv_gdp.merge(&dv_life).unwrap();
    println!("{}", dv);
    // println!("{}", serde_json::to_string(&dv).unwrap());
}
