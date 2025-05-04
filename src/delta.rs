use deltalake::{
    kernel::{
        Action, Add, Invariant, LogicalFile, Remove, StructType, Transaction, scalars::ScalarExt,
    },
    operations::optimize::OptimizeType,
};
use polars_lazy::frame::{LazyFrame, ScanArgsParquet};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use std::{
    path::{Path, PathBuf},
    vec,
};

use arrow::array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::{FsOpCallback, LoadData, MyDirEntry, Store};
use deltalake::DeltaOps;
use deltalake::{DeltaTable, arrow::array::RecordBatch};
pub struct SaveToDelta {
    table: DeltaTable,

    schema: Schema,
}
impl SaveToDelta {
    pub fn new(uri: &str) -> Result<Self> {
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);

        let runtime = deltalake::storage::IORuntime::default().get_handle();
        let jh = runtime.spawn(deltalake::DeltaTableBuilder::from_uri(uri).load());
        let table: DeltaTable = match runtime.block_on(jh)? {
            Ok(table) => table,
            Err(_) => {
                let table = deltalake::DeltaTableBuilder::from_uri(uri).build()?;
                let ops = DeltaOps(table);

                let sch: StructType = (&schema).try_into()?;

                let jh = runtime.spawn(
                    ops.create()
                        .with_columns(sch.fields().cloned())
                        .into_future(),
                );

                runtime.block_on(jh)??
            }
        };
        //table.update().await?;

        Ok(SaveToDelta { schema, table })
    }
}

impl Store for SaveToDelta {
    fn reader(&self) -> impl LoadData {
        DeltaReader {
            table: self.table.clone(),
        }
    }

    fn writer(&self) -> impl FsOpCallback {
        DeltaWriter {
            table: self.table.clone(),
            queue: vec![],
            schema: self.schema.clone(),
        }
    }
}

pub struct DeltaReader {
    table: DeltaTable,
}
pub struct DeltaWriter {
    queue: vec::Vec<MyDirEntry>,
    schema: Schema,
    table: DeltaTable,
}
use polars::prelude::*;
impl LoadData for DeltaReader {
    fn load(&mut self, callback: &mut dyn FsOpCallback) -> Result<()> {
        //self.table.update().await.expect("update ok");

        let Ok(files) = self.table.get_file_uris() else {
            return Ok(());
        };

        let vec: Vec<PathBuf> = files.map(|x| PathBuf::from(x)).collect();
        let a = vec.into_boxed_slice();

        let Ok(df) = LazyFrame::scan_parquet_files(a.into(), ScanArgsParquet::default()) else {
            return Ok(());
        };
        let Ok(a) = df.collect() else {
            return Ok(());
        };
        for row in 0..a.height() {
            let row_content = a.get_row(row).expect("bad row");
            let AnyValue::String(s) = row_content.0[0] else {
                continue;
            };
            callback.on_op(MyDirEntry {
                path: PathBuf::from(s),
            })?
        }

        Ok(())
    }
}

impl FsOpCallback for DeltaWriter {
    fn on_op(&mut self, entry: MyDirEntry) -> Result<()> {
        self.queue.push(entry);

        if self.queue.len() > 1000 {
            self.flush()?
        }

        Ok(())

        //tokio::runtime::Handle::current().
    }
    fn flush(&mut self) -> Result<()> {
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

        let runtime = deltalake::storage::IORuntime::default().get_handle();

        //let join = runtime.spawn(
        runtime.spawn(DeltaOps(self.table.clone()).write([batch]).into_future());
        //DeltaOps(self.table.clone()).optimize().with_type(OptimizeType::Compact).await?;
        self.queue.clear();
        Ok(())
    }
}
