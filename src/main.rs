use glob::glob;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::zip;
use std::path::Path;
use std::path::PathBuf;
use threadpool::ThreadPool;
use std::sync::mpsc::sync_channel;
use clap::Parser;
use named_tuple::named_tuple;
use chrono::{DateTime};



use rusqlite::{params, Connection, Result, ToSql};


/// Build a database
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    ///Path to the log entries to show in table
    #[clap(short, long)]
    logpath: String,

    ///Path to the novelty scores for each log entry
    #[clap(short, long)]
    noveltypath: String,

    ///Path to the sqlite database file
    #[clap(long)]
    db: String
}

struct LogData {
    // timestamp: DateTime<Utc>,
    timestamp_str: String,
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
    let glob_pattern = PathBuf::from(path)
        .join("*.csv")
        .to_str()
        .unwrap()
        .to_string();

    // println!("host glob: {}", &glob_pattern);
    // let mut glob_pattern = String::from(path);
    // glob_pattern.push_str("*.csv");

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

    let glob_pattern = PathBuf::from(path)
        .join(format!("{}{}", hostname, ".*.csv"))
        .to_str().unwrap()
        .to_string();

    // println!("svc glob: {}", &glob_pattern);

    // let mut glob_pattern = String::from(path);
    // glob_pattern.push_str(hostname);
    // glob_pattern.push_str(".*.csv");

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

            let svcname_stripped = svcname
                .strip_prefix("csv_").unwrap_or(svcname);
            let svcname_stripped = svcname_stripped
                .strip_suffix("_").unwrap_or(svcname_stripped);

            data.get_mut(hostname).unwrap()
                .insert(svcname_stripped.to_string(), CsvPaths::new(cur_log_path, cur_novelty_path));

        }
    }
    return data;
}


fn get_relevant_data(log_path: &PathBuf, novelty_path: &PathBuf) -> Vec<LogData> {
    // println!("{:?}, {:?}", log_path, novelty_path);
    let mut log_data = Vec::new();

    let mut log_reader = csv::ReaderBuilder::new()
        .from_path(log_path)
        .unwrap();
    let mut novelty_reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_path(novelty_path)
        .unwrap();

    let log_headers = log_reader.headers().unwrap();
    let novelty_headers = novelty_reader.headers().unwrap();

    // println!("Log headers: {:?}", log_headers);
    // println!("Novelty headers: {:?}", novelty_headers);

    let log_datetime_idx = log_headers.iter().position(|r| r == "datetime").unwrap();
    let log_message_idx = log_headers.iter().position(|r| r == "message").unwrap();
    let novelty_datetime_idx = novelty_headers.iter().position(|r| r == "datetime").unwrap();
    let novelty_sum_scores_idx = novelty_headers.iter().position(|r| r == "sum scores")
        .expect(&*format!("Couldn't find sum scores header in {:?}", novelty_path));

    // let mut counter = 1;
    for (log, novelty) in zip(log_reader.records(), novelty_reader.records()) {
        let log_record = log.unwrap();
        let novelty_record = novelty.unwrap();

        // println!("datetime? {}", log_record.get(1).unwrap());

        let cur_log_datetime = DateTime::parse_from_rfc3339(
            log_record.get(log_datetime_idx).unwrap()
        );
        let cur_log_datetime = match cur_log_datetime {
            Ok(datetime) => datetime,
            Err(error) => {
                println!("Warning: Couldn't parse log datetime {}, {}", log_record.get(log_datetime_idx).unwrap(), error);
                continue;
            }
        };

        let cur_novelty_datetime = DateTime::parse_from_rfc3339(
            log_record.get(novelty_datetime_idx).unwrap()
        );
        let cur_novelty_datetime = match cur_novelty_datetime {
            Ok(datetime) => datetime,
            Err(error) => {
                println!("Warning: Couldn't parse novelty datetime {}, {}", log_record.get(novelty_datetime_idx).unwrap(), error);
                continue;
            }
        };

        assert_eq!(cur_novelty_datetime, cur_log_datetime);

        // println!("log {:?}", cur_datetime);

        let cur_msg = match log_record.get(log_message_idx) {
            None => {
                println!("Couldn't parse message: {:?}", log_record);
                continue;
            },
            Some(x) => String::from(x)
        };

        let cur_novelty_score: Result<f32, _> = match novelty_record.get(novelty_sum_scores_idx) {
            None => { println!("Couldn't parse novelty score : {:?}", novelty_record); continue; },
            Some(x) => {
                let tmp: String = x.chars().filter(|c| !c.is_whitespace()).collect();
                tmp.parse()
            }
        };
        let cur_novelty_score = match cur_novelty_score {
            Ok(x) => x,
            Err(error) => { println!("Couldn't parse float: {:?}, {}", novelty_record, error); continue;}
        };

        let cur_logdata = LogData {
            // timestamp: cur_log_datetime.with_timezone(&Utc),
            timestamp_str: cur_log_datetime.format("%F %T%.6f").to_string(),
            message: cur_msg,
            novelty_score: cur_novelty_score
        };

        log_data.push(cur_logdata);

    }

    // return counter;
    return log_data;
}

fn main() {
    let args = Args::parse();

    // Database
    let mut conn = Connection::open(args.db).unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
             PRAGMA synchronous = 0;
             PRAGMA cache_size = 1000000;
             PRAGMA locking_mode = EXCLUSIVE;
             PRAGMA temp_store = MEMORY;
             PRAGMA defer_foreign_keys = TRUE;"
    )
    .unwrap();

    // Get the service id for all services, so we can insert loglines with the correct service later
    let mut service_id_map: HashMap<String, HashMap<String, i32>> = HashMap::new();
    {
        let mut stmt = conn.prepare("
            SELECT service.id, service.name, host.name
            FROM service
            JOIN host ON service.host_id = host.id
        ").unwrap();

        let rows = stmt.query_map([], |row| {
            let svc_id: i32 = row.get(0).unwrap();
            let svcname: String = row.get(1).unwrap();
            let hostname: String = row.get(2).unwrap();
            return Ok((svc_id, svcname, hostname));
        }).unwrap();

        for row in rows {
            let (svc_id, svcname, hostname) = row.unwrap();
            println!("{}, {}, {}", svc_id, svcname, hostname);
            let h_map = service_id_map.entry(hostname).or_insert(HashMap::new());
            h_map.insert(svcname, svc_id);
        }
    }
    //service_id_map.insert(String::from(row.get(1)?), String::from(row.get(0)?))
    // println!("{:?}", service_id_map);

    println!("Getting hosts and services paths...");
    let data = get_hosts_and_services(&args.logpath, &args.noveltypath);

    println!("Building database...");
    let mut n_jobs = 0;

    // Set up reader workers
    let n_reader_workers = 4; //num_cpus::get() - 1;
    let reader_pool = ThreadPool::new(n_reader_workers);
    let (reader_tx, reader_rx) = sync_channel(8);

    for hostname in data.keys() {
        for svcname in data.get(hostname).unwrap().keys() {
            println!("Loading {}, {}", hostname, svcname);
            let paths = data[hostname][svcname].clone();
            let tx = reader_tx.clone();

            let svc_hostname_map = match service_id_map.get(hostname) {
                None => {
                    panic!("Couldn't find service id for hostname: {}, (service: {})", hostname, svcname);
                }
                Some(x) => x
            };

            let svc_id = match svc_hostname_map.get(svcname) {
                None => {
                    panic!("Couldn't find service id for service: {}, (hostname: {})", svcname, hostname);
                }
                Some(x) => x.clone()
            };

            n_jobs += 1;
            reader_pool.execute(move|| {
                let result = get_relevant_data(
                    paths.log_path(),
                    paths.novelty_path()
                );
                tx.send((result, svc_id)).unwrap();
            });
        }
    }

    let mut jobs_finished = 0;

    let transaction = conn.transaction().unwrap();

    let chunksize = 50;

    {
        let tx = &transaction;

        // Prepare single line insert query
        let mut insert_query_single = tx.prepare_cached("
            INSERT INTO logline (timestamp, message, novelty_score, service_id) VALUES (?1, ?2, ?3, ?4)
        ").unwrap();

        // Prepare batch insert query
        let mut query_params = " (?, ?, ?, ?),".repeat(chunksize);
        query_params.pop();
        let query_str= format!("INSERT INTO logline (timestamp, message, novelty_score, service_id) VALUES {}", query_params);
        let mut insert_query = tx.prepare_cached(query_str.as_str()).unwrap();

        // Receive data and insert
        while jobs_finished != n_jobs {
            let (cur_data, svc_id) = reader_rx.recv().unwrap();

            let n_chunks = cur_data.len() / chunksize;
            let n_rest = cur_data.len() - (n_chunks * chunksize);

            // Insert data in chunks of chunksize
            for i in 0..n_chunks {
                let mut sql_params: Vec<_> = Vec::new();

                for data in &cur_data[i*chunksize..(i+1)*chunksize] {
                    sql_params.push(&data.timestamp_str as &dyn ToSql);
                    sql_params.push(&data.message as &dyn ToSql);
                    sql_params.push(&data.novelty_score as &dyn ToSql);
                    sql_params.push(&svc_id as &dyn ToSql);
                }

                insert_query.execute(&*sql_params).unwrap();
            }

            // Insert remaining data, if any
            if n_rest > 0 {
                for data in &cur_data[n_chunks*chunksize..] {
                    insert_query_single.execute(params![
                        data.timestamp_str,
                        data.message,
                        data.novelty_score,
                        svc_id
                    ]).unwrap();
                }
            }

            jobs_finished += 1;
            println!("[{}/{}] {}", jobs_finished, n_jobs, cur_data.len());
        }
    }

    println!("Committing...");
    transaction.commit().unwrap();

    conn.close().unwrap();

    reader_pool.join();

    println!("Done");

}