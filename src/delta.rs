use async_trait::async_trait;
use deltalake::kernel::{
    Action, Add, Invariant, LogicalFile, Remove, StructType, Transaction, scalars::ScalarExt,
};
use std::sync::Arc;

use anyhow::Result;
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

#[async_trait]
impl LoadData for SaveToDelta {
    async fn load(&mut self, callback: &mut dyn FsOpCallback) -> Result<()> {
        self.table.update().await.expect("update ok");

        Ok(())
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
