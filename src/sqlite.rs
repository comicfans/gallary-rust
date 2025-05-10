use crate::common::{FsOpCallback, Zoned};
use std::{path::PathBuf, vec};

use crate::common::{BasicPicture, PictureRecord, Store, StoreReader};
use anyhow::Result;
use rusqlite::{Connection, Rows, Statement};
pub struct SaveToSqlite {
    conn: Connection,
    path: PathBuf,
}

pub struct SqliteWriter {
    conn: Connection,
    queue: vec::Vec<PictureRecord>,
}

pub struct SqliteReader {
    conn: Connection,
    path: PathBuf,
}

use ouroboros::self_referencing;
#[self_referencing]
struct ConnStmt {
    conn: Connection,
    #[borrows(conn)]
    #[covariant]
    stmt: Statement<'this>,
}

#[self_referencing]
pub struct SqliteResult {
    error_count: usize,
    conn_stmt: ConnStmt,
    #[borrows(mut conn_stmt)]
    #[covariant]
    rows: Rows<'this>,
}

unsafe impl Send for SqliteResult {}

impl SaveToSqlite {
    pub fn new(path: PathBuf) -> Result<Self> {
        let conn = Connection::open(path.clone())?;
        conn.execute(
            "create table if not exists records(path,fs_create_time_timestamp, fs_create_time_timezone)",
            (),
        )
        .expect("create ok");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("wal ok");
        Ok(SaveToSqlite { conn, path })
    }
}
impl Store for SaveToSqlite {
    fn writer(self: &Self) -> impl FsOpCallback {
        let ret = SqliteWriter {
            conn: Connection::open(self.path.clone()).expect("open"),
            queue: vec![],
        };

        ret.conn
            .execute_batch("PRAGMA journal_mode=WAL;")
            .expect("wal ok");

        ret
    }

    fn reader(self: &Self) -> impl StoreReader {
        SqliteReader {
            conn: Connection::open(self.path.clone()).expect("open"),
            path: self.path.clone(),
        }
    }
}

fn as_sqlite_tuple(record: &PictureRecord) -> Result<(&str, String, String)> {
    let fs_create_time_timestamp = serde_json::to_value(&record.fs_create_time.0.timestamp())?;

    let fs_create_time_timestamp = fs_create_time_timestamp.as_str().expect("as_str");

    let fs_create_time_timezone = record
        .fs_create_time
        .0
        .time_zone()
        .iana_name()
        .expect("no timezone"); //TODO
    Ok((
        &record.path,
        fs_create_time_timestamp.to_owned(),
        fs_create_time_timezone.to_owned(),
    ))
}

impl FsOpCallback for SqliteWriter {
    fn on_op(&mut self, entry: PictureRecord) -> Result<()> {
        self.queue.push(entry);

        if self.queue.len() > 1000 {
            self.flush()?
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.conn.execute_batch("begin transaction")?;
        for entry in self.queue.iter() {
            if let Ok(tup) = as_sqlite_tuple(entry) {
                self.conn
                    .execute(
                        "insert into records(path, fs_create_time_timestamp, fs_create_time_timezone) values (?1,?2,?3)",
                        tup,
                    )
                    .expect("insert ok");
            }
        }
        self.conn.execute_batch("commit;").expect("commit ok");
        //TODO: lose queue content or grow infinitely
        self.queue.clear();
        Ok(())
    }
}

impl SqliteReader {
    pub fn new(path: PathBuf) -> Result<Self> {
        let conn = Connection::open(path.clone())?;

        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("wal ok");
        Ok(SqliteReader { conn, path })
    }
}

use crate::common::OrderBy;
impl StoreReader for SqliteReader {
    fn load(
        &mut self,
        order_by: OrderBy,
        limit: usize,
    ) -> Result<impl Iterator<Item = BasicPicture>> {
        let conn = Connection::open(self.path.clone())?;
        Ok(SqliteResultBuilder {
            error_count: 0,
            conn_stmt: ConnStmtBuilder {
                conn,
                stmt_builder: |conn: &Connection| {
                    conn.prepare(
                        &("SELECT * from records order by  ".to_owned()
                            + match order_by {
                                OrderBy::FsCreateTime => "fs_create_time_timestamp",
                                OrderBy::FsModifyTime => "fs_create_time_timestamp", // TODO
                                OrderBy::ExifCreateTime => "fs_create_time_timestamp", // TODO
                            }
                            + &match limit {
                                0 => "".to_owned(),
                                _ => " limit ".to_owned() + &limit.to_string(),
                            }),
                    )
                    .expect("prepare")
                },
            }
            .build(),
            rows_builder: |conn_stmt: &mut ConnStmt| {
                conn_stmt
                    .with_stmt_mut(|stmt| stmt.query([]))
                    .expect("query")
            },
        }
        .build())
    }
}

const MAX_RETRY: usize = 10;
impl Iterator for SqliteResult {
    type Item = BasicPicture;

    fn next(&mut self) -> Option<Self::Item> {
        while self.with_error_count(|error_count| {
            return *error_count < MAX_RETRY;
        }) {
            match self.with_rows_mut(|rows| rows.next()) {
                Ok(None) => {
                    return None;
                }

                Ok(Some(row)) => {
                    let path: std::result::Result<String, _> = row.get(0);
                    let Ok(path) = path else {
                        continue;
                    };

                    let fs_create_time_timestamp: std::result::Result<String, _> = row.get(1);
                    let Ok(fs_create_time_timestamp) = fs_create_time_timestamp else {
                        continue;
                    };

                    let Ok(fs_create_time_timestamp) =
                        fs_create_time_timestamp.parse::<jiff::Timestamp>()
                    else {
                        continue;
                    };

                    let fs_create_time_timezone: std::result::Result<String, _> = row.get(2);
                    let Ok(fs_create_time_timezone) = fs_create_time_timezone else {
                        continue;
                    };

                    let Ok(fs_create_time_timezone) =
                        jiff::tz::TimeZone::get(&fs_create_time_timezone)
                    else {
                        continue;
                    };

                    return Some(BasicPicture {
                        path,
                        fs_create_time: Zoned(jiff::Zoned::new(
                            fs_create_time_timestamp,
                            fs_create_time_timezone,
                        )),
                        exif_create_time: None,
                    });
                }
                _ => {}
            }

            self.with_error_count_mut(|error_count| {
                *error_count += 1;
            })
        }

        None
    }
}
