use crate::FsOpCallback;
use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    vec,
};

use crate::{LoadData, MyDirEntry};
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
unsafe impl Send for SqliteReader {}

impl SaveToSqlite {
    pub fn new(path: PathBuf) -> Result<Self> {
        let conn = Connection::open(path.clone())?;
        conn.execute("create table if not exists records(path)", ())
            .expect("create ok");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("wal ok");
        Ok(SaveToSqlite { conn, path })
    }

    pub fn writer(self: &Self) -> SqliteWriter {
        SqliteWriter {
            conn: Connection::open(self.path.clone()).expect("open"),
            queue: vec![],
        }
    }

    pub fn reader(self: &Self) -> SqliteReader {
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
            self.conn.execute(
                "insert into records(path) values (?1)",
                (entry.path().to_string_lossy().to_string(),),
            )?;
        }
        self.conn.execute_batch("commit;")?;
        //TODO: lose queue content or grow infinitely
        self.queue.clear();
        Ok(())
    }
}

impl LoadData for SqliteReader {
    fn load(&mut self, callback: &mut dyn FsOpCallback) -> Result<()> {
        let mut stmt = self.conn.prepare("SELECT * from records")?;
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
