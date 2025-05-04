use anyhow::Result;
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

trait FsOpCallback: Send {
    fn on_op(&mut self, path: MyDirEntry) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

trait LoadData: Send {
    fn load(&mut self, callback: &mut dyn FsOpCallback) -> Result<()>;
}

trait Store {
    fn reader(&self) -> impl LoadData;
    fn writer(&self) -> impl FsOpCallback;
}

fn walk_files(root: &Path, callback: &mut dyn FsOpCallback) -> Result<()> {
    let walker = WalkDir::new(root);

    for entry in walker.into_iter() {
        match entry {
            Ok(entry) => {
                callback.on_op(MyDirEntry {
                    path: entry.path().to_owned(),
                })?;
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
    use rand::RngCore;
    use std::vec::Vec;

    fn get_walk_dir() -> String {
        std::env::var("WALK_DIR")
            .unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned() + "/target".into())
    }

    struct RandomPathGenerator {
        rng: rand::rngs::SmallRng,
        current: Vec<(String, u32)>,
        depth: u32,
        width: u32,
    }

    impl RandomPathGenerator {
        fn random_string(rng: &mut rand::rngs::SmallRng) -> String {
            let charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            let gen_length = rng.next_u32() % ((charset.len() - 1) as u32) + 1;

            (0..gen_length)
                .into_iter()
                .map(|_| {
                    let idx = rng.next_u32() % (charset.len() as u32);
                    charset.chars().nth(idx as usize).unwrap()
                })
                .collect()
        }

        fn new(depth: u32, width: u32) -> Result<Self> {
            if depth == 0 || width == 0 {
                return Err(anyhow::anyhow!("depth and width must be greater than 0"));
            }
            use rand::SeedableRng;

            let mut rng = rand::rngs::SmallRng::seed_from_u64(0);

            let mut current = vec![];
            for _ in 0..depth {
                current.push((RandomPathGenerator::random_string(&mut rng), 1));
            }
            *current.last_mut().unwrap() = ("".into(), 0);

            Ok(RandomPathGenerator {
                rng,
                current,
                depth,
                width,
            })
        }
    }

    impl Iterator for RandomPathGenerator {
        type Item = String;
        fn next(self: &mut Self) -> Option<String> {
            let rightmost_adv = self.current.iter().rposition(|x| x.1 < self.width)?;

            let adv_res = self.current[rightmost_adv].1 + 1;
            for i in rightmost_adv..self.depth as usize {
                self.current[i] = (RandomPathGenerator::random_string(&mut self.rng), 1);
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

    fn test_no_save() {
        walk_files(Path::new(&get_walk_dir()), &mut ()).expect("walk success");
    }

    use crate::delta::SaveToDelta;
    use crate::sqlite::SaveToSqlite;
    use counter::Counter;

    struct Checker {
        result: Counter<String>,
    }
    impl FsOpCallback for Checker {
        fn on_op(&mut self, path: MyDirEntry) -> Result<()> {
            self.result[&path.path.to_string_lossy().into_owned()] += 1;
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn test_write_read_compare(store: impl Store) {
        writer_benchmark(&mut store.writer());
        let mut reader = store.reader();
        let mut checker = Checker {
            result: Counter::<String>::new(),
        };
        reader.load(&mut checker).expect("read ok");
        let expect = rand_path_generator().into_iter().collect::<Counter<_>>();

        assert_eq!(checker.result, expect);
    }
    #[test]
    #[named]
    fn test_deltalake_write_read_compare() {
        let deltalake = SaveToDelta::new(function_name!()).expect("ok");
        test_write_read_compare(deltalake);
    }
    #[test]
    #[named]
    fn test_sqlite_write_read_compare() {
        let sqlite_store =
            SaveToSqlite::new(PathBuf::from(function_name!())).expect("sqlite create");
        test_write_read_compare(sqlite_store);
    }

    #[test]
    #[named]
    fn test_write_delta() {
        let delta = SaveToDelta::new(function_name!().into()).expect("ok");
        walk_files(Path::new(&get_walk_dir()), &mut delta.writer()).expect("walk success");
    }
    #[test]
    #[named]
    fn test_sqlite_writes() {
        let save_to_sqlite =
            SaveToSqlite::new(PathBuf::from(function_name!())).expect("sqlite create");
        walk_files(Path::new(&get_walk_dir()), &mut save_to_sqlite.writer()).expect("walk success");
    }

    fn rand_path_generator() -> RandomPathGenerator {
        RandomPathGenerator::new(3, 80).expect("gen random")
    }

    fn writer_benchmark(callback: &mut dyn FsOpCallback) {
        for path in rand_path_generator() {
            callback
                .on_op(MyDirEntry { path: path.into() })
                .expect("ok");
        }
        callback.flush().expect("flush ok");
    }

    #[test]
    #[named]
    fn test_sqlite_benchmark() {
        let save_to_sqlite =
            SaveToSqlite::new(PathBuf::from(function_name!())).expect("sqlite create");
        writer_benchmark(&mut save_to_sqlite.writer());
    }

    #[test]
    #[named]
    fn test_deltalake_benchmark() {
        let delta = SaveToDelta::new(function_name!().into()).expect("ok");
        writer_benchmark(&mut delta.writer());
    }

    fn do_read_10(save: &mut dyn LoadData) {
        let mut callback = ();
        for _ in 0..10 {
            save.load(&mut callback).expect("read ok");
        }
    }

    fn multi_read_single_writer_benchmark(store: impl Store) {
        let mut loader = store.reader();
        let mut save = store.writer();
        use std::sync::{Arc, Condvar, Mutex};
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let start = Arc::new((Mutex::new(0), Condvar::new()));

        let sync_wait = || {
            {
                let (lock, cvar) = &*start;

                let mut started = lock.lock().unwrap();
                *started += 1;
                cvar.notify_one();
            }

            {
                let (lock, cvar) = &*pair.clone();
                let mut started = lock.lock().unwrap();
                while !*started {
                    started = cvar.wait(started).unwrap();
                }
            }
        };

        std::thread::scope(|s| {
            s.spawn(|| {
                sync_wait();
                writer_benchmark(&mut save);
            });

            s.spawn(|| {
                sync_wait();
                do_read_10(&mut loader);
            });

            s.spawn(|| {
                {
                    let (lock, cvar) = &*start;
                    let mut started = lock.lock().unwrap();
                    while *started != 2 {
                        started = cvar.wait(started).unwrap();
                    }
                }
                {
                    let (lock, cvar) = &*pair;
                    let mut started = lock.lock().unwrap();
                    *started = true;
                    cvar.notify_all();
                }
            });
        });
    }

    #[named]
    #[test]
    fn test_sqlite_rw_benchmark() {
        let save_to_sqlite =
            SaveToSqlite::new(PathBuf::from(function_name!())).expect("sqlite create");

        multi_read_single_writer_benchmark(save_to_sqlite);
    }

    #[named]
    #[test]
    fn test_deltalake_rw_benchmark() {
        let save_to_delta = SaveToDelta::new(function_name!()).expect("deltalake create");
        multi_read_single_writer_benchmark(save_to_delta);
    }
}
