use crate::common::FsOpCallback;
use std::{
    mem::MaybeUninit,
    path::{Path, PathBuf},
    rc::Rc,
    vec,
};

use crate::common::{MyDirEntry, Store, StoreReader};
use anyhow::Result;
use rusqlite::{Connection, Row, Rows, Statement};
pub struct SaveToSqlite {
    conn: Connection,
    path: PathBuf,
}

pub struct SqliteWriter {
    conn: Connection,
    queue: vec::Vec<MyDirEntry>,
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
        conn.execute("create table if not exists records(path)", ())
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

impl FsOpCallback for SqliteWriter {
    fn on_op(&mut self, entry: MyDirEntry) -> Result<()> {
        self.queue.push(entry);

        if self.queue.len() > 1000 {
            self.flush()?
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.conn.execute_batch("begin transaction")?;
        for entry in self.queue.iter() {
            self.conn
                .execute(
                    "insert into records(path) values (?1)",
                    (entry.path().to_string_lossy().to_string(),),
                )
                .expect("insert ok");
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
    ) -> Result<impl Iterator<Item = MyDirEntry>> {
        let conn = Connection::open(self.path.clone())?;
        Ok(SqliteResultBuilder {
            error_count: 0,
            conn_stmt: ConnStmtBuilder {
                conn,
                stmt_builder: |conn: &Connection| {
                    conn.prepare("SELECT * from records limit 100")
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

impl Iterator for SqliteResult {
    type Item = MyDirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let MAX_RETRY = 10;

        while self.with_error_count(|error_count| {
            return *error_count < MAX_RETRY;
        }) {
            match self.with_rows_mut(|rows| rows.next()) {
                Ok(None) => {
                    return None;
                }

                Ok(Some(row)) => {
                    let str: String = row.get(0).unwrap_or("".to_owned());

                    if str != "" {
                        return Some(MyDirEntry {
                            path: PathBuf::from(str),
                        });
                    }
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
