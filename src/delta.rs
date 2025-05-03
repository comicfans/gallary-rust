use async_trait::async_trait;
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

use crate::{FsOpCallback, LoadData, MyDirEntry};
use deltalake::DeltaOps;
use deltalake::{DeltaTable, arrow::array::RecordBatch};
pub struct SaveToDelta {
    queue: vec::Vec<MyDirEntry>,

    table: DeltaTable,

    schema: Schema,
}
impl SaveToDelta {
    pub async fn new(uri: &str) -> Result<Self> {
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

use polars::prelude::*;
#[async_trait(?Send)]
impl LoadData for SaveToDelta {
    async fn load(&self, callback: &mut dyn FsOpCallback) -> Result<()> {
        //self.table.update().await.expect("update ok");

        let Ok(files) = self.table.get_file_uris() else {
            return Ok(());
        };

        let vec: Vec<PathBuf> = files.map(|x| PathBuf::from(x)).collect();
        let a = vec.into_boxed_slice();

        let df = LazyFrame::scan_parquet_files(a.into(), ScanArgsParquet::default())?;
        let a = df.collect()?;
        for row in 0..a.height() {
            let row_content = a.get_row(row).expect("bad row");
            let AnyValue::String(s) = row_content.0[0] else {
                continue;
            };
            callback
                .on_op(MyDirEntry {
                    path: PathBuf::from(s),
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait(?Send)]
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

        DeltaOps(self.table.clone()).write([batch]).await?;
        //DeltaOps(self.table.clone()).optimize().with_type(OptimizeType::Compact).await?;
        self.queue.clear();
        Ok(())
    }
}
