use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::time;

use super::Backend;
use noria::{DataType, SyncTable};

fn do_put<'a>(mutator: &'a mut SyncTable, tx: bool) -> Box<FnMut(Vec<DataType>) + 'a> {
    if tx {
        // Box::new(move |v| assert!(mutator.transactional.insert(v, Token::empty()).is_ok())),
        unimplemented!()
    } else {
        Box::new(move |v| assert!(mutator.insert(v).is_ok()))
    }
}

fn populate_table(backend: &mut Backend, data: &Path, use_txn: bool) -> usize {
    use std::str::FromStr;

    let table_name = data.file_stem().unwrap().to_str().unwrap();
    let mut putter = backend.g.table(table_name).unwrap().into_sync();

    let f = File::open(data).unwrap();
    let mut reader = BufReader::new(f);

    let mut s = String::new();
    println!("Populating {}...", table_name);
    let start = time::Instant::now();
    let mut i = 0;
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split('\t').map(str::trim).collect();
            let rec: Vec<DataType> = fields
                .into_iter()
                .map(|s| match i64::from_str(s) {
                    Ok(v) => v.into(),
                    Err(_) => s.into(),
                })
                .collect();
            do_put(&mut putter, use_txn)(rec);
        }
        i += 1;
        s.clear();
    }
    let dur = start.elapsed().as_float_secs();
    println!(
        "Inserted {} {} records in {:.2}s ({:.2} PUTs/sec)!",
        i,
        table_name,
        dur,
        f64::from(i) / dur
    );
    i as usize
}

pub fn populate(backend: &mut Backend, data_location: &str, use_txn: bool) -> io::Result<()> {
    use std::fs;

    let dir = Path::new(data_location);
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                populate_table(backend, &path, use_txn);
            }
        }
    }
    Ok(())
}
