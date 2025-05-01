use deltalake::{DeltaTable, arrow::array::RecordBatch};
use rusqlite::Connection;

use anyhow::Result;
use arrow::array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_walkdir::DirEntry;
use async_walkdir::WalkDir;
use chrono::TimeDelta;
use deltalake::DeltaOps;
use futures_lite::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    vec,
};

use async_trait::async_trait;

struct MyDirEntry {
    path: PathBuf,
}

impl MyDirEntry {
    fn path(self: &Self) -> PathBuf {
        self.path.clone()
    }
}

#[async_trait]
trait FsOpCallback {
    async fn on_op(&mut self, path: MyDirEntry) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
}

struct SaveToDelta {
    queue: vec::Vec<MyDirEntry>,

    table: DeltaTable,

    schema: Schema,
}

async fn walk_files(root: &Path, callback: &mut dyn FsOpCallback) -> Result<()> {
    let mut walker = WalkDir::new(root);

    loop {
        match walker.next().await {
            Some(Ok(entry)) => {
                callback.on_op(MyDirEntry { path: entry.path() }).await?;
            }
            Some(Err(_)) => break,
            None => break,
        }
    }

    callback.flush().await
}
use deltalake::kernel::{
    Action, Add, Invariant, LogicalFile, Remove, StructType, Transaction, scalars::ScalarExt,
};
impl SaveToDelta {
    async fn new(uri: &str) -> Result<Self> {
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);

        let table = match deltalake::DeltaTableBuilder::from_uri(uri).load().await {
            Ok(table) => table,
            Err(_) => {
                let table = deltalake::DeltaTableBuilder::from_uri(uri).build()?;
                let ops = DeltaOps(table);

                let sch: StructType = (&schema).try_into()?;

                ops.create()
                    .with_columns(sch.fields().cloned())
                    .await
                    .expect("build ok")
            }
        };
        //table.update().await?;

        Ok(SaveToDelta {
            queue: vec![],
            schema,
            table,
        })
    }
}

#[async_trait]
impl FsOpCallback for SaveToDelta {
    async fn on_op(&mut self, entry: MyDirEntry) -> Result<()> {
        self.queue.push(entry);

        if self.queue.len() > 1000 {
            self.flush().await?
        }

        Ok(())

        //tokio::runtime::Handle::current().
    }
    async fn flush(&mut self) -> Result<()> {
        if self.queue.is_empty() {
            return Ok(());
        }

        let vec: vec::Vec<String> = self
            .queue
            .iter()
            .map(|entry| entry.path().to_string_lossy().into_owned())
            .collect();

        let string_array = StringArray::from(vec);

        let batch =
            RecordBatch::try_new(Arc::new(self.schema.clone()), vec![Arc::new(string_array)])
                .unwrap();

        match DeltaOps(self.table.clone()).write([batch]).await {
            Ok(_) => {
                self.queue.clear();
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

struct SaveToSqlite {
    conn: Connection,
    queue: vec::Vec<MyDirEntry>,
}

impl SaveToSqlite {
    fn new(path: PathBuf) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute("create table if not exists records(path)", ())
            .expect("create ok");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("wal ok");
        Ok(SaveToSqlite {
            conn,
            queue: vec![],
        })
    }
}

#[async_trait]
impl FsOpCallback for SaveToSqlite {
    async fn on_op(&mut self, entry: MyDirEntry) -> Result<()> {
        self.queue.push(entry);

        if self.queue.len() > 1000 {
            self.flush().await?
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.conn.execute_batch("begin transaction")?;
        for entry in self.queue.iter() {
            self.conn.execute(
                "insert into records(path) values (?1)",
                (entry.path().to_string_lossy().to_string(),),
            )?;
        }
        self.conn.execute_batch("commit;")?;
        self.queue.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use function_name::named;

    fn get_walk_dir() -> String {
        std::env::var("WALK_DIR")
            .unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned() + "/target".into())
    }

    struct RandomPathGenerator {
        current: vec::Vec<(String, u32)>,
        depth: u32,
        width: u32,
    }

    use random_string::generate;
    impl RandomPathGenerator {
        fn new(depth: u32, width: u32) -> Result<Self> {
            if depth == 0 || width == 0 {
                return Err(anyhow::anyhow!("depth and width must be greater than 0"));
            }

            let mut current = vec![];
            for _ in 0..depth {
                let charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                let random_str = generate(5, charset);
                current.push((random_str, 1));
            }
            *current.last_mut().unwrap() = ("".into(), 0);

            Ok(RandomPathGenerator {
                current,
                depth,
                width,
            })
        }

        fn next(self: &mut Self) -> Option<String> {
            let rightmost_adv = self.current.iter().rposition(|x| x.1 < self.width)?;

            let adv_res = self.current[rightmost_adv].1 + 1;
            for i in rightmost_adv..self.depth as usize {
                self.current[i] = (
                    generate(
                        5,
                        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                    ),
                    1,
                );
            }
            self.current[rightmost_adv].1 = adv_res;

            let mut ret: String = "".into();

            for (str, _) in self.current.iter() {
                ret.push(std::path::MAIN_SEPARATOR);
                ret.push_str(str);
            }
            Some(ret)
        }
    }
    #[async_trait]
    impl FsOpCallback for () {
        async fn on_op(&mut self, _path: MyDirEntry) -> Result<()> {
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn test_no_save() {
        walk_files(Path::new(&get_walk_dir()), &mut ())
            .await
            .expect("walk success");
    }

    #[tokio::test]
    async fn test_write_delta() {
        let mut delta = SaveToDelta::new("deltalake".into()).await.expect("ok");
        walk_files(Path::new(&get_walk_dir()), &mut delta)
            .await
            .expect("walk success");
    }
    #[tokio::test]
    async fn test_sqlite_writes() {
        let mut save_to_sqlite =
            SaveToSqlite::new(PathBuf::from("sqlite.db")).expect("sqlite create");
        walk_files(Path::new(&get_walk_dir()), &mut save_to_sqlite)
            .await
            .expect("walk success");
    }

    fn rand_path_generator() -> RandomPathGenerator {
        RandomPathGenerator::new(10, 50).expect("gen random")
    }

    #[tokio::test]
    async fn test_sqlite_benchmark() {
        let mut save_to_sqlite =
            SaveToSqlite::new(PathBuf::from("benchmark.sqlite")).expect("sqlite create");

        let mut generator = rand_path_generator();

        while let Some(path) = generator.next() {
            save_to_sqlite
                .on_op(MyDirEntry { path: path.into() })
                .await
                .expect("ok");
        }
    }

    #[tokio::test]
    async fn test_deltalake_benchmark() {
        let mut delta = SaveToDelta::new("benchmark.deltalake".into())
            .await
            .expect("ok");

        let mut generator = rand_path_generator();
        while let Some(path) = generator.next() {
            delta
                .on_op(MyDirEntry { path: path.into() })
                .await
                .expect("ok");
        }
    }
}
