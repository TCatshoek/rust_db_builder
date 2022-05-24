use glob::glob;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::zip;
use std::num::ParseFloatError;
use std::path::Path;
use std::path::PathBuf;
use threadpool::ThreadPool;
use std::sync::mpsc::sync_channel;
use clap::Parser;
use named_tuple::named_tuple;
use chrono::{Utc, DateTime, ParseResult, ParseError, FixedOffset};
use std::fs::File;
use std::thread;
use polars::df;
use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::prelude::{CsvReader, DataType, Field};
use polars::series::Series;
use rusqlite::{params, Connection, Result};
use itertools::izip;
use polars::export::arrow::datatypes::Schema;

/// Build a database
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    ///Path to the log entries to show in table
    #[clap(short, long)]
    logpath: String,

    ///Path to the novelty scores for each log entry
    #[clap(short, long)]
    noveltypath: String
}

struct LogData {
    timestamp: DateTime<Utc>,
    message: String,
    novelty_score: f32
}

named_tuple!(
    #[derive(Clone, Debug)]
    struct CsvPaths {
        log_path: PathBuf,
        novelty_path: PathBuf,
    }
);


fn get_hostnames(path: &str) -> HashSet<String> {

    let mut glob_pattern = String::from(path);
    glob_pattern.push_str("*.csv");

    let csv_file_paths = glob(&glob_pattern).expect("Failed to parse glob pattern");

    let mut hostnames = HashSet::new();

    for csv_pathresult in csv_file_paths {

        let csv_path = csv_pathresult.unwrap();
        let filename = csv_path
            .file_stem().unwrap()
            .to_str().unwrap()
            .split(".")
            .next().unwrap();

        hostnames.insert(filename.to_string());
    }

    return hostnames;
}

fn get_svcnames(hostname: &str, path: &str) -> HashSet<String> {

    let mut glob_pattern = String::from(path);
    glob_pattern.push_str(hostname);
    glob_pattern.push_str(".*.csv");

    let csv_file_paths = glob(&glob_pattern).expect("Failed to parse glob pattern");

    let mut svcnames = HashSet::new();

    for csv_pathresult in csv_file_paths {

        let csv_path = csv_pathresult.unwrap();
        let filename = csv_path
            .file_stem().unwrap()
            .to_str().unwrap()
            .split(".")
            .skip(1).next().unwrap();

        svcnames.insert(filename.to_string());
    }

    return svcnames;
}

fn get_hosts_and_services(logpath: &str, noveltypath: &str) -> HashMap<String, HashMap<String, CsvPaths>> {
    println!("Log path: {}", logpath);
    println!("Novelty path: {}", noveltypath);

    let hostnames_log = get_hostnames(logpath);
    let hostnames_novelty = get_hostnames(noveltypath);
    let in_both: Vec<_> = hostnames_log
        .intersection(&hostnames_novelty).collect();
    let not_in_both: Vec<_> = hostnames_log
        .symmetric_difference(&hostnames_novelty).collect();

    if not_in_both.len() > 0 {
        panic!("Some hosts are not available in both folders: {:?}", not_in_both);
    }

    let mut data: HashMap<String, HashMap<String, CsvPaths>> = HashMap::new();

    for hostname in in_both {
        data.insert(hostname.clone(), HashMap::new());

        let services_log = get_svcnames(hostname, logpath);
        let services_novelty = get_svcnames(hostname, noveltypath);
        let svc_in_both: Vec<_> = services_log
            .intersection(&services_novelty).collect();
        let svc_not_in_both: Vec<_> = services_log
            .symmetric_difference(&services_novelty).collect();

        if svc_not_in_both.len() > 0 {
            panic!("Some services are not available in both folders: {:?}", svc_not_in_both);
        }

        for svcname in svc_in_both {

            let cur_log_path = Path::new(logpath)
                .join(&format!("{}.{}.evtx_full_.csv", hostname, svcname));

            let cur_novelty_path = Path::new(noveltypath)
                .join(&format!("{}.{}.evtx_symbols_.csv.res.csv", hostname, svcname));

            assert!(cur_novelty_path.exists());
            assert!(cur_log_path.exists());

            data.get_mut(hostname).unwrap()
                .insert(svcname.clone(), CsvPaths::new(cur_log_path, cur_novelty_path));

        }
    }
    return data;
}

fn strip_column_name_whitespace(df: &mut DataFrame) {
    let mut new_column_names = Vec::new();

    for mut column_name in df.get_column_names_owned() {
        column_name = column_name.trim().to_string();
        // column_name.retain(|c| !c.is_whitespace());
        new_column_names.push(column_name);
    }

    df.set_column_names(&new_column_names).unwrap();
}

fn get_host_service_dataframes(log_path: &PathBuf, novelty_path: &PathBuf) -> (DataFrame, DataFrame) {

    println!("{:?}", log_path);
    let log_columns: Vec<String> = Vec::from([
        String::from("datetime"),
        String::from("message")
    ]);

    let mut df_log = CsvReader::from_path(log_path)
        .unwrap()
        .has_header(true)
        .infer_schema(Some(100))
        .with_n_threads(Some(1))
        .with_parse_dates(true)
        // .with_columns(log_columns)
        .finish()
        .unwrap();
        // .drop_nulls(None)
        // .unwrap();

    strip_column_name_whitespace(&mut df_log);
    println!("{}", df_log.head(Some(5)));

    df_log = df_log.select(log_columns).unwrap();

    let novelty_columns: Vec<String> = Vec::from([
        String::from("datetime"),
        String::from("sum scores")
    ]);

    let mut df_novelty = CsvReader::from_path(novelty_path)
        .unwrap()
        .infer_schema(Some(100))
        .with_delimiter(b';')
        .has_header(true)
        .with_parse_dates(true)
        .with_n_threads(Some(1))
        // .with_columns(novelty_columns)
        .finish()
        .unwrap();
        // .drop_nulls(None)
        // .unwrap();

    strip_column_name_whitespace(&mut df_novelty);
    // println!("{:?}", df_novelty.head(None));
    df_novelty = df_novelty.select(novelty_columns).unwrap();
    println!("{}", df_novelty.head(Some(5)));
    // println!("{:?}", df_novelty.head(Some(5)));

    return (df_log, df_novelty);

}

fn get_relevant_data(log_path: &PathBuf, novelty_path: &PathBuf) -> Vec<LogData> {
    // println!("{:?}, {:?}", log_path, novelty_path);
    let mut log_data = Vec::new();

    let mut log_reader = csv::Reader::from_path(log_path).unwrap();
    let mut novelty_reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_path(novelty_path)
        .unwrap();

    let mut counter = 1;
    for (log, novelty) in zip(log_reader.records(), novelty_reader.records()) {
        let log_record = log.unwrap();
        let novelty_record = novelty.unwrap();

        // println!("datetime? {}", log_record.get(1).unwrap());

        let cur_datetime = DateTime::parse_from_rfc3339(
            log_record.get(1).unwrap()
        );
        let cur_datetime = match cur_datetime {
            Ok(datetime) => datetime,
            Err(error) => {
                println!("Warning: Couldn't parse datetime {}", log_record.get(1).unwrap());
                continue;
            }
        };

        // println!("log {:?}", cur_datetime);

        let cur_msg = match log_record.get(5) {
            None => {
                println!("Couldn't parse message: {:?}", log_record);
                continue;
            },
            Some(x) => String::from(x)
        };

        let cur_novelty_score: Result<f32, _> = match novelty_record.get(1) {
            None => { println!("Couldn't parse novelty score : {:?}", novelty_record); continue; },
            Some(x) => {
                let tmp: String = x.chars().filter(|c| !c.is_whitespace()).collect();
                tmp.parse()
            }
        };
        let cur_novelty_score = match cur_novelty_score {
            Ok(x) => x,
            Err(error) => { println!("Couldn't parse float: {:?}", novelty_record); continue;}
        };

        let cur_logdata = LogData {
            timestamp: cur_datetime.with_timezone(&Utc),
            message: cur_msg,
            novelty_score: cur_novelty_score
        };

        log_data.push(cur_logdata);

        counter += 1;
    }

    // return counter;
    return log_data;
}

fn main() {
    let args = Args::parse();

    // Database
    let conn = Connection::open("test.db").unwrap();

    conn.execute(
        "DROP TABLE IF EXISTS logline", []
    ).unwrap();

    conn.execute(
        "CREATE TABLE logline (
                id              INTEGER PRIMARY KEY,
                datetime        TEXT,
                message         TEXT,
                novelty_score   REAL
                )",
        []
    ).unwrap();

    println!("Getting hosts and services...");
    let data = get_hosts_and_services(&args.logpath, &args.noveltypath);

    println!("Building database...");
    let mut n_jobs = 0;

    // Set up reader workers
    let n_reader_workers = 1;
    let reader_pool = ThreadPool::new(n_reader_workers);
    let (reader_tx, reader_rx) = sync_channel(0);

    for hostname in data.keys() {
        for svcname in data.get(hostname).unwrap().keys() {
            let paths = data[hostname][svcname].clone();
            let tx = reader_tx.clone();
            n_jobs += 1;
            reader_pool.execute(move|| {
                let result = get_host_service_dataframes(
                    paths.log_path(),
                    paths.novelty_path()
                );
                tx.send(result).unwrap();
            });
        }
    }

    // Set up joining workers
    let n_joiner_workers = 1;
    let joiner_pool = ThreadPool::new(n_joiner_workers);
    let (joiner_tx, joiner_rx) = sync_channel(0);

    let mut reader_jobs_finished = 0;
    let reader_recv_thread = thread::spawn(move || {
        while reader_jobs_finished != n_jobs {
            let (df_log, df_novelty) = reader_rx.recv().unwrap();
            reader_jobs_finished += 1;

            let tx = joiner_tx.clone();
            joiner_pool.execute(move|| {
                // println!("log: \n{}", df_log.head(Some(5)));
                // println!("novelty: \n{}", df_novelty.head(Some(5)));
                let df_joined = df_log.left_join(&df_novelty, ["datetime"], ["datetime"]).unwrap();
                tx.send(df_joined).unwrap();
            });
        }
    });



    let mut joiner_jobs_finished = 0;
    let joiner_recv_thread = thread::spawn(move || {
        while joiner_jobs_finished != n_jobs {

            let joined_df = joiner_rx.recv().unwrap();



            // println!("{:?}", joined_df.head(None));

            joiner_jobs_finished += 1;

            let sumscores_column = joined_df.column("sum scores").unwrap().f64();
            let sumscores_column = match sumscores_column {
                Ok(x) => x,
                Err(_) => continue
            };

            let zipped_columns = izip!(
                joined_df.column("datetime").unwrap().datetime().unwrap().into_iter(),
                joined_df.column("message").unwrap().utf8().unwrap().into_iter(),
                // sumscores_column.into_iter()
            );

            let mut lines = 0;
            for (datetime, message) in zipped_columns {
                lines += 1;
                println!("lines: {}", lines);

                let d = match datetime {
                    Some(x) => x,
                    None => continue
                };

                let m = match message {
                    Some(x) => x,
                    None => continue
                };

                // let n = match novelty_score {
                //     Some(x) => x,
                //     None => continue
                // };
                // println!("{} {} {}", d, m, n);
                conn.execute(
                    "INSERT INTO logline (datetime, message, novelty_score) VALUES (?1, ?2, ?3)",
                    params![d, m, 1]
                ).unwrap();
            }






            println!("{}/{} shape: {:?}", joiner_jobs_finished, n_jobs, joined_df.shape());
            // println!("{:?}", joined_df.shape());
        }
    });

    reader_recv_thread.join().unwrap();
    joiner_recv_thread.join().unwrap();

}