use anyhow::Result;
use futures_lite::stream::StreamExt;
use std::path::{Path, PathBuf};

mod delta;
mod sqlite;
use walkdir::WalkDir;

pub struct MyDirEntry {
    path: PathBuf,
}

impl MyDirEntry {
    fn path(self: &Self) -> PathBuf {
        self.path.clone()
    }
}

//#[trait_variant::make(FsOpCallback: Send)]
trait FsOpCallback {
    fn on_op(&mut self, path: MyDirEntry) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

//#[trait_variant::make(LoadData: Send)]
trait LoadData {
    fn load(&mut self, callback: &mut dyn FsOpCallback) -> Result<()>;
}

fn walk_files(root: &Path, callback: &mut dyn FsOpCallback) -> Result<()> {
    let walker = WalkDir::new(root);

    for entry in walker.into_iter(){
        match entry{
            Ok(entry) => {
                callback.on_op(MyDirEntry { path: entry.path().to_owned() })?;
            }
            Err(_) => break,
        }
    }


    callback.flush()
}

#[cfg(test)]

mod tests {

    use super::*;
    use function_name::named;
    use std::vec::Vec;

    fn get_walk_dir() -> String {
        std::env::var("WALK_DIR")
            .unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned() + "/target".into())
    }

    struct RandomPathGenerator {
        current: Vec<(String, u32)>,
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
    #[async_trait::async_trait]
    impl FsOpCallback for () {
        fn on_op(&mut self, _path: MyDirEntry) -> Result<()> {
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    //impl LoadData for () {
    //    async fn load(&mut self, _: &mut dyn FsOpCallback) -> Result<()> {
    //        Ok(())
    //    }
    //}
    fn test_no_save() {
        walk_files(Path::new(&get_walk_dir()), &mut ())
            .expect("walk success");
    }

    use crate::delta::SaveToDelta;
    use crate::sqlite::SaveToSqlite;

    #[test]
    #[named]
    fn test_write_delta() {
        let delta = SaveToDelta::new(function_name!().into()).expect("ok");
        walk_files(Path::new(&get_walk_dir()), delta.writer().as_mut())
            .expect("walk success");
    }
    #[test]
    #[named]
    fn test_sqlite_writes() {
        let save_to_sqlite =
            SaveToSqlite::new(PathBuf::from(function_name!())).expect("sqlite create");
        walk_files(Path::new(&get_walk_dir()), &mut save_to_sqlite.writer())
            .expect("walk success");
    }

    fn rand_path_generator() -> RandomPathGenerator {
        RandomPathGenerator::new(3, 100).expect("gen random")
    }

    fn writer_benchmark(callback: &mut dyn FsOpCallback) {
        let mut generator = rand_path_generator();

        while let Some(path) = generator.next() {
            callback
                .on_op(MyDirEntry { path: path.into() })
                .expect("ok");
        }
    }

    #[test]
    #[named]
    fn test_sqlite_benchmark() {
        let save_to_sqlite =
            SaveToSqlite::new(PathBuf::from(function_name!())).expect("sqlite create");
        writer_benchmark(&mut save_to_sqlite.writer());
    }

    //#[tokio::test]
    //#[named]
    //async fn test_deltalake_benchmark() {
    //    let delta = SaveToDelta::new(function_name!().into()).await.expect("ok");
    //    writer_benchmark(delta.writer().as_mut()).await;
    //}

    fn do_write(save: &mut dyn FsOpCallback) {
        let mut generator = rand_path_generator();
        while let Some(path) = generator.next() {
            save.on_op(MyDirEntry { path: path.into() }).expect("ok");
        }
    }

    fn do_read_10(save: &mut dyn LoadData) {
        let mut callback = ();
        for _ in 0..10 {
            save.load(&mut callback).expect("read ok");
        }
    }

    #[named]
    #[test]
    fn test_multi_read_single_write() {
        let save_to_sqlite =
            SaveToSqlite::new(PathBuf::from(function_name!())).expect("sqlite create");

        let mut reader = save_to_sqlite.reader();
        let mut writer = save_to_sqlite.writer();
        let write_task = std::thread::spawn(move || {
            writer_benchmark(&mut writer);
        });

        let read_task = std::thread::spawn(move || {
            do_read_10(&mut reader);
        });

        write_task.join();
        read_task.join();
    }    }

