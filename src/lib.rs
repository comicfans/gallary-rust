use deltalake::{arrow::array::{RecordBatch,record_batch}, DeltaTable, DeltaTableError, ObjectStoreError};
use walkdir::{DirEntry, WalkDir};

use deltalake::DeltaOps;
use std::{path::PathBuf, path::Path,vec};
use tokio::runtime::Handle;

trait FsOpCallback{
    fn on_op(&mut self,path: &DirEntry)->Result<(),DeltaTableError>;
}

struct SaveToDelta{

    queue: vec::Vec<DirEntry>,

    table_uri: String,
}

fn walk_files(root: &Path, callback:&mut dyn FsOpCallback)->Result<(),DeltaTableError>{

    let walker = WalkDir::new(root);



    for entry in walker.into_iter().filter_map(|e| e.ok()) {
        callback.on_op(&entry)?;
    }

    Ok(())

}

impl SaveToDelta{
    
    async fn flush(&mut self)->Result<(),DeltaTableError>{

        DeltaOps::try_from_uri(self.table_uri.clone()).await?.write(
record_batch!(
    ("a", Int32, [1, 2, 3]),
    ("b", Float64, [Some(4.0), None, Some(5.0)]),
    ("c", Utf8, ["alpha", "beta", "gamma"]))
        ).with_table_name("default");

        Ok(())
    }
}

impl FsOpCallback for SaveToDelta{

    fn on_op(&mut self, entry: &DirEntry) ->Result<(),DeltaTableError>{
        self.queue.push(entry.clone());

        if self.queue.len() > 100 {
            Handle::current().block_on(async {
                self.flush().await
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    use function_name::named;
    use tokio::runtime::Runtime;

    //#[tokio::test]
    #[test]
    #[named]
    fn test_speed(){

        let mut dir = Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
        dir.push(function_name!());
        if let Err(_) = std::fs::create_dir(&dir){
                std::fs::remove_dir_all(&dir).expect("remove dir failed");
                std::fs::create_dir(&dir).expect("create dir failed");
        }



        let runtime = Runtime::new().unwrap();
        let join_handle = runtime.spawn_blocking(move||{
        let mut delta = SaveToDelta{
            queue: vec![],
            table_uri: dir.to_str().unwrap().to_string(),
        };
        walk_files(Path::new("/home/comicfans"),&mut delta).expect("walk success");
        });

        runtime.block_on(join_handle).expect("walk success");
    }


}
