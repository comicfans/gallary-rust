use deltalake::{arrow::array::{RecordBatch}, DeltaTable};
use rusqlite::{Connection};

use deltalake::DeltaOps;
use std::{path::{Path, PathBuf}, sync::OnceLock, vec};
use async_walkdir::DirEntry;
use std::sync::Arc;
use anyhow::Result;
use tokio::runtime::Runtime;
use async_walkdir::WalkDir;
use futures_lite::{stream::StreamExt,future::block_on};
use chrono::TimeDelta;
use std::time::Duration;
use arrow_schema::{Schema,Field,DataType};
use arrow::array::{Int32Array,StringArray};

use tokio::runtime::{Builder as RuntimeBuilder, Handle};
fn io_rt() -> &'static Runtime {
    static IO_RT: OnceLock<Runtime> = OnceLock::new();
    IO_RT.get_or_init(|| {

                    //RuntimeBuilder::new_current_thread().build().expect("create")
tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build().expect("Failed to create a tokio runtime.")

    })
}
use async_trait::async_trait;

#[async_trait]
trait FsOpCallback{
    async fn on_op(&mut self,path: &DirEntry)->Result<()>;
    async fn flush(&mut self)->Result<()>;
}

struct SaveToDelta{

    queue: vec::Vec<DirEntry>,

    table: DeltaTable,

    schema: Schema,
}

async fn walk_files(root: &Path, callback:&mut dyn FsOpCallback)->Result<()>{

    let mut walker = WalkDir::new(root);

    loop {
        match walker.next().await {
            Some(Ok(entry)) => {

                callback.on_op(&entry).await?;
            }
            Some(Err(_)) => break,
            None=>break,
        }
    }

    callback.flush().await
}
use deltalake::kernel::{
    scalars::ScalarExt, Action, Add, Invariant, LogicalFile, Remove, StructType, Transaction,
};
impl SaveToDelta{

    async fn new(uri:&str)->Result<Self>{

        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, false)]);

        let table = match deltalake::DeltaTableBuilder::from_uri(uri).load().await
        {
            Ok(table)=>{
                table
            }
            Err(_)=>{
                let table = deltalake::DeltaTableBuilder::from_uri(uri). build()?;
                let ops = DeltaOps(table);


                let sch: StructType = (&schema).try_into()?;

                ops.create()
                    .with_columns(sch.fields().cloned())
                    .await.expect("build ok")
            }

        };
        //table.update().await?;


        Ok(SaveToDelta{
            queue:vec![],
            schema,
            table,


        })
    }



}


#[async_trait]
impl FsOpCallback for SaveToDelta{

    async fn on_op(&mut self, entry: &DirEntry) ->Result<()>{
        self.queue.push(entry.clone());

        if self.queue.len() > 1000{
            self.flush().await?
        }

        Ok(())

        //tokio::runtime::Handle::current(). 
    }
    async fn flush(&mut self)->Result<()>{

        if self.queue.is_empty(){
            return Ok(());
        }


        let vec:vec::Vec<String> = self.queue.iter().map(|entry|{entry.path().to_string_lossy().into_owned()}).collect();

        let string_array = StringArray::from(vec);

        let batch = RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![Arc::new(string_array)]
        ).unwrap();

        match DeltaOps(self.table.clone()).write(
            [batch]
        ).await{

            Ok(_)=>{

                self.queue.clear();
                Ok(())
            }
            Err(e)=>Err(e.into())

        }
    }
}

struct SaveToSqlite{
    conn: Connection,
    queue: vec::Vec<DirEntry>,
}

impl SaveToSqlite{
    fn new(path:PathBuf)->Result<Self>{
        let conn = Connection::open(path)?;
        conn.execute("create table if not exists records(path)",()).expect("create ok");
        conn.execute_batch("PRAGMA journal_mode=WAL;").expect("wal ok");
        Ok(SaveToSqlite{
            conn,
            queue:vec![]
        })
    }
}

#[async_trait]
impl FsOpCallback for SaveToSqlite{
async fn on_op(&mut self,entry: &DirEntry)->Result<()> {
        self.queue.push(entry.clone());

        if self.queue.len() > 1000{
            self.flush().await?
        }

        Ok(())
    }

    async fn flush(&mut self)->Result<()>{
        self.conn.execute_batch("begin transaction")?;
        for entry in self.queue.iter(){
            self.conn.execute("insert into records(path) values (?1)", (entry.path().to_string_lossy().to_string(),))?;
        }
        self.conn.execute_batch("commit;")?;
        self.queue.clear();
        Ok(())
    }
}


#[cfg(test)]
mod tests{

    use super::*;
    use function_name::named;


    fn get_walk_dir()->String{

        std::env::var("WALK_DIR").unwrap_or_else(|_|env!("CARGO_MANIFEST_DIR").to_owned()+"/target".into())
    }

    #[async_trait]
    impl FsOpCallback for (){
        async fn on_op(&mut self,_path: &DirEntry)->Result<()> {
            Ok(())
        }

        async fn flush(&mut self)->Result<()> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn test_no_save(){
        walk_files(Path::new(&get_walk_dir()), &mut ()).await.expect("walk success");
    }

    #[tokio::test]
    async fn test_write_delta(){

        let mut delta= SaveToDelta::new("deltalake".into()).await.expect("ok");
        walk_files(Path::new(&get_walk_dir()),&mut delta).await.expect("walk success");
    }
    #[tokio::test]
    async fn test_sqlite_writes(){


        let mut save_to_sqlite = SaveToSqlite::new(PathBuf::from("sqlite.db")).expect("sqlite create");
        walk_files(Path::new(&get_walk_dir()),&mut save_to_sqlite).await.expect("walk success");


    }

}
