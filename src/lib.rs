use rusqlite::{params, Connection};
use smol_str::format_smolstr;
use std::path::PathBuf;

struct LogDB {
    connection: Connection,
    count: usize,
}

const LOG_INSERT_STATEMENT: &str = "INSERT INTO log (type, message) VALUES (?, ?)";

static mut BATCH_SIZE: usize = 0;

impl LogDB {
    fn new(connection: Connection) -> Self {
        LogDB {
            connection,
            count: 0,
        }
    }

    fn add(&mut self, record: &log::Record) {
        self.count += 1;
        let mut cached_statement = self
            .connection
            .prepare_cached(LOG_INSERT_STATEMENT)
            .unwrap();
        let level_usize = record.metadata().level() as usize;
        let str = format_smolstr!("{}", record.args());

        cached_statement
            .execute(params![level_usize, str.as_str()])
            .unwrap();
        if self.count >= unsafe { BATCH_SIZE } {
            self.count = 0;
            self.connection.execute_batch("COMMIT; BEGIN").unwrap()
        }
    }

    fn flush(&self) {
        self.connection.execute_batch("COMMIT; BEGIN").unwrap();
    }
}

struct Logger {
    conn: std::sync::Mutex<LogDB>,
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        self.conn.lock().unwrap().add(record);
    }

    fn flush(&self) {
        self.conn.lock().unwrap().flush();
    }
}

pub fn setup_logger(path: PathBuf) {
    let log_file = path.join("log.sqlite");
    let conn = rusqlite::Connection::open(log_file.clone()).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS log (
        id INTEGER PRIMARY KEY,
        type INTEGER,
        message TEXT NOT NULL
    )",
        (),
    )
    .unwrap();
    let statment = conn
        .prepare_cached("INSERT INTO log (type, message) VALUES (?, ?)")
        .unwrap();
    drop(statment);
    conn.execute("BEGIN", ()).unwrap();
    let logger = Logger {
        conn: std::sync::Mutex::new(LogDB::new(conn)),
    };
    let boxed = Box::new(logger);
    log::set_logger(Box::leak(boxed)).unwrap();
    log::set_max_level(log::LevelFilter::Info);
    println!("Log file {:?}", log_file);
}

pub fn test_in_dir(batch_size: usize) {
    unsafe {
        BATCH_SIZE = batch_size;
    }
    let system_time = std::time::SystemTime::now();

    for i in 0..1000000 {
        log::info!("Hello, world! {}", i);
    }

    let log_ref = log::logger();

    log_ref.flush();

    let elapsed = system_time.elapsed().unwrap();

    println!("Batch size {} Elapsed time: {:?}", batch_size, elapsed);
}

pub fn test_path(path: PathBuf) {
    setup_logger(path);
    let iter_counts = [
        10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
    ];
    for &count in iter_counts.iter() {
        test_in_dir(count);
    }
}
