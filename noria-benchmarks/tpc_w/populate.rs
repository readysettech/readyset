use chrono::naive::NaiveDate;
use chrono::naive::NaiveDateTime;
use chrono::naive::NaiveTime;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use std::thread;
use std::time;

use super::Backend;
use noria::DataType;

fn populate(backend: &mut Backend, name: &'static str, mut records: Vec<Vec<DataType>>) -> usize {
    let mut mutator = backend.g.table(name).unwrap().into_sync();

    let i = records.len();

    let mut do_prepop = move || {
        let start = time::Instant::now();

        let i = records.len();
        for r in records.drain(..) {
            mutator.insert(r).unwrap();
        }

        let dur = start.elapsed().as_float_secs();
        println!(
            "Inserted {} {} in {:.2}s ({:.2} PUTs/sec)!",
            i,
            name,
            dur,
            i as f64 / dur
        );
    };

    if backend.parallel_prepop {
        let barrier = backend.barrier.clone();

        thread::spawn(move || {
            barrier.wait();
            do_prepop();
            barrier.wait();
        });
    } else {
        do_prepop();
    }

    i
}

fn parse_ymd_to_timestamp(s: &str) -> i64 {
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
    let ts = d.and_time(NaiveTime::from_hms(0, 0, 0)).timestamp();
    ts as i64
}

pub fn populate_addresses(backend: &mut Backend, data_location: &str) -> usize {
    let f = File::open(format!("{}/addresses.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating addresses...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let addr_id = i32::from_str(fields[0]).unwrap();
            let addr_street1 = fields[1];
            let addr_street2 = fields[2];
            let addr_city = fields[3];
            let addr_state = fields[4];
            let addr_zip = fields[5];
            let addr_co_id = i32::from_str(fields[6]).unwrap();
            records.push(vec![
                addr_id.into(),
                addr_street1.into(),
                addr_street2.into(),
                addr_city.into(),
                addr_state.into(),
                addr_zip.into(),
                addr_co_id.into(),
            ]);
        }
        s.clear();
    }

    populate(backend, "ship", records.clone());
    populate(backend, "bill", records.clone());
    populate(backend, "address", records)
}

pub fn populate_authors(
    backend: &mut Backend,
    data_location: &str,
    write: f32,
    start: bool,
) -> usize {
    let f = File::open(format!("{}/authors.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating authors...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let a_id = i32::from_str(fields[0]).unwrap();
            let a_fname = fields[1];
            let a_lname = fields[2];
            let a_mname = fields[3];
            let a_dob = parse_ymd_to_timestamp(fields[4]);
            let a_bio = fields[5];
            records.push(vec![
                a_id.into(),
                a_fname.into(),
                a_lname.into(),
                a_mname.into(),
                a_dob.into(),
                a_bio.into(),
            ]);
        }
        s.clear();
    }

    scale_records(&mut records, start, write);

    populate(backend, "author", records)
}

pub fn populate_cc_xacts(backend: &mut Backend, data_location: &str) -> usize {
    let f = File::open(format!("{}/cc_xacts.data", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating cc_xacts...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let cx_o_id = i32::from_str(fields[0]).unwrap();
            let cx_type = fields[1];
            let cx_num = fields[2];
            let cx_name = fields[3];
            let cx_expire = parse_ymd_to_timestamp(fields[4]);
            let cx_auth_id = fields[5];
            let cx_amt = f64::from_str(fields[6]).unwrap();
            let xact_date = NaiveDateTime::parse_from_str(fields[7], "%Y-%m-%d %H:%M:%S");
            let cx_xact_date = xact_date.unwrap().timestamp();
            let cx_co_id = i32::from_str(fields[8]).unwrap();
            records.push(vec![
                cx_o_id.into(),
                cx_type.into(),
                cx_num.into(),
                cx_name.into(),
                cx_expire.into(),
                cx_auth_id.into(),
                cx_amt.into(),
                cx_xact_date.into(),
                cx_co_id.into(),
            ]);
        }
        s.clear();
    }

    populate(backend, "cc_xacts", records)
}

pub fn populate_countries(backend: &mut Backend, data_location: &str) -> usize {
    let f = File::open(format!("{}/countries.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating countries...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let co_id = i32::from_str(fields[0]).unwrap();
            let co_name = fields[1];
            let co_exchange = f64::from_str(fields[2]).unwrap();
            let co_currency = fields[3];
            records.push(vec![
                co_id.into(),
                co_name.into(),
                co_exchange.into(),
                co_currency.into(),
            ]);
        }
        s.clear();
    }

    populate(backend, "ship_co", records.clone());
    populate(backend, "bill_co", records.clone());
    populate(backend, "country", records)
}

pub fn populate_customers(backend: &mut Backend, data_location: &str) -> usize {
    let f = File::open(format!("{}/customers.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating customers...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let c_id = i32::from_str(fields[0]).unwrap();
            let c_uname = fields[1];
            let c_passwd = fields[2];
            let c_fname = fields[3];
            let c_lname = fields[4];
            let c_addr_id = i32::from_str(fields[5]).unwrap();
            let c_phone = fields[6];
            let c_email = fields[7];
            let c_since = parse_ymd_to_timestamp(fields[8]);
            let c_last_login = parse_ymd_to_timestamp(fields[9]);
            let c_login = fields[10];
            let c_expiration = fields[11];
            let c_discount = f64::from_str(fields[12]).unwrap();
            let c_balance = f64::from_str(fields[13]).unwrap();
            let c_ytd_pmt = f64::from_str(fields[14]).unwrap();
            let c_birthdate = parse_ymd_to_timestamp(fields[15]);
            let c_data = fields[16];
            records.push(vec![
                c_id.into(),
                c_uname.into(),
                c_passwd.into(),
                c_fname.into(),
                c_lname.into(),
                c_addr_id.into(),
                c_phone.into(),
                c_email.into(),
                c_since.into(),
                c_last_login.into(),
                c_login.into(),
                c_expiration.into(),
                c_discount.into(),
                c_balance.into(),
                c_ytd_pmt.into(),
                c_birthdate.into(),
                c_data.into(),
            ]);
        }
        s.clear();
    }

    populate(backend, "customer", records)
}

pub fn populate_items(
    backend: &mut Backend,
    data_location: &str,
    write: f32,
    start: bool,
) -> usize {
    let f = File::open(format!("{}/items.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating items...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let i_id = i32::from_str(fields[0]).unwrap();
            let i_title = fields[1];
            let i_a_id = i32::from_str(fields[2]).unwrap();
            let i_pub_date = parse_ymd_to_timestamp(fields[3]);
            let i_publisher = fields[4];
            let i_subject = fields[5];
            let i_desc = fields[6];
            let i_related1 = i32::from_str(fields[7]).unwrap();
            let i_related2 = i32::from_str(fields[8]).unwrap();
            let i_related3 = i32::from_str(fields[9]).unwrap();
            let i_related4 = i32::from_str(fields[10]).unwrap();
            let i_related5 = i32::from_str(fields[11]).unwrap();
            let i_thumbnail = fields[12];
            let i_image = fields[13];
            let i_srp = f64::from_str(fields[14]).unwrap();
            let i_cost = fields[15];
            let i_avail = parse_ymd_to_timestamp(fields[16]);
            let i_stock = i32::from_str(fields[17]).unwrap();
            let i_isbn = fields[18];
            let i_page = i32::from_str(fields[19]).unwrap();
            let i_backing = fields[20];
            let i_dimensions = fields[21];
            records.push(vec![
                i_id.into(),
                i_title.into(),
                i_a_id.into(),
                i_pub_date.into(),
                i_publisher.into(),
                i_subject.into(),
                i_desc.into(),
                i_related1.into(),
                i_related2.into(),
                i_related3.into(),
                i_related4.into(),
                i_related5.into(),
                i_thumbnail.into(),
                i_image.into(),
                i_srp.into(),
                i_cost.into(),
                i_avail.into(),
                i_stock.into(),
                i_isbn.into(),
                i_page.into(),
                i_backing.into(),
                i_dimensions.into(),
            ]);
        }
        s.clear();
    }

    scale_records(&mut records, start, write);

    populate(backend, "item", records)
}

pub fn populate_orders(backend: &mut Backend, data_location: &str) -> usize {
    let f = File::open(format!("{}/orders.tsv", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating orders...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let o_id = i32::from_str(fields[0]).unwrap();
            let o_c_id = i32::from_str(fields[1]).unwrap();
            let o_date = NaiveDateTime::parse_from_str(fields[2], "'%Y-%m-%d %H:%M:%S'")
                .unwrap()
                .timestamp();
            let o_sub_total = f64::from_str(fields[3]).unwrap();
            let o_tax = f64::from_str(fields[4]).unwrap();
            let o_total = f64::from_str(fields[5]).unwrap();
            let o_ship_type = fields[6];
            let o_ship_date = NaiveDateTime::parse_from_str(fields[7], "'%Y-%m-%d %H:%M:%S'")
                .unwrap()
                .timestamp();
            let o_bill_addr_id = i32::from_str(fields[8]).unwrap();
            let o_ship_addr_id = i32::from_str(fields[9]).unwrap();
            let o_status = fields[10];

            records.push(vec![
                o_id.into(),
                o_c_id.into(),
                o_date.into(),
                o_sub_total.into(),
                o_tax.into(),
                o_total.into(),
                o_ship_type.into(),
                o_ship_date.into(),
                o_bill_addr_id.into(),
                o_ship_addr_id.into(),
                o_status.into(),
            ]);
        }
        s.clear();
    }

    populate(backend, "orders", records)
}

pub fn populate_order_line(
    backend: &mut Backend,
    data_location: &str,
    write: f32,
    start: bool,
) -> usize {
    let f = File::open(format!("{}/order_line.data", data_location)).unwrap();
    let mut reader = BufReader::new(f);

    println!("Prepopulating order_line...");

    let mut s = String::new();
    let mut records = Vec::new();
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let ol_id = i32::from_str(fields[0]).unwrap();
            let ol_o_id = i32::from_str(fields[1]).unwrap();
            let ol_i_id = i32::from_str(fields[2]).unwrap();
            let ol_qty = i32::from_str(fields[3]).unwrap();
            let ol_discount = f64::from_str(fields[4]).unwrap();
            let ol_comments = fields[5];

            records.push(vec![
                ol_id.into(),
                ol_o_id.into(),
                ol_i_id.into(),
                ol_qty.into(),
                ol_discount.into(),
                ol_comments.into(),
            ]);
        }
        s.clear();
    }

    scale_records(&mut records, start, write);

    populate(backend, "order_line", records)
}

fn scale_records(records: &mut Vec<Vec<DataType>>, start: bool, write: f32) {
    let nrecords = if start {
        ((records.len() as f32) * write) as usize
    } else {
        records.reverse();
        ((records.len() as f32) * (1.0 - write)) as usize
    };

    records.truncate(nrecords);
}
