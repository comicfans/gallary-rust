use crate::common::FsOpCallback;
use std::{
    path::{Path, PathBuf},
    vec,
};

use crate::common::{MyDirEntry, Store, StoreReader};
use anyhow::Result;
use rusqlite::Connection;
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
}

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

use crate::common::OrderBy;
impl StoreReader for SqliteReader {
    fn load(
        &mut self,
        order_by: OrderBy,
        limit: usize,
        callback: &mut dyn FsOpCallback,
    ) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare(&("SELECT * from records limit ".to_owned() + &limit.to_string()))?;
        let person_iter = stmt.query_map([], |row| {
            let str: String = row.get(0)?;
            Ok(MyDirEntry {
                path: PathBuf::from(str),
            })
        })?;

        for entry in person_iter {
            callback.on_op(entry?)?;
        }

        Ok(())
    }
}
