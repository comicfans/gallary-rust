pub mod common;
#[cfg(feature = "delta")]
pub mod delta;
pub mod http;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{
        BasicPicture, FsOpCallback, OrderBy, PictureRecord, Store, StoreReader, walk_files,
    };

    use anyhow::Result;
    use function_name::named;
    use rand::RngCore;
    use std::path::{Path, PathBuf};
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

    impl FsOpCallback for () {
        fn on_op(&mut self, _path: PictureRecord) -> Result<()> {
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn test_no_save() {
        walk_files(Path::new(&get_walk_dir()), &mut ()).expect("walk success");
    }

    //use crate::delta::SaveToDelta;

    use crate::sqlite::SaveToSqlite;
    use counter::Counter;

    struct Checker {
        result: Counter<String>,
    }
    impl FsOpCallback for Checker {
        fn on_op(&mut self, picture: PictureRecord) -> Result<()> {
            self.result[&picture.path.to_owned()] += 1;
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn test_write_read_compare(store: impl Store) {
        writer_benchmark(&mut store.writer());
        let mut reader = store.reader();
        let mut checker = Counter::<String>::new();
        let res = reader.load(OrderBy::FsModifyTime, 0).expect("read ok");
        for v in res {
            checker[&v.path] += 1;
        }
        let expect = rand_path_generator().into_iter().collect::<Counter<_>>();

        assert_eq!(checker, expect);
    }
    #[test]
    #[named]
    #[cfg(feature = "delta")]
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
    #[cfg(feature = "delta")]
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
                .on_op(PictureRecord {
                    path: path.into(),
                    fs_create_time: common::Zoned(jiff::Zoned::now()),
                })
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
    #[cfg(feature = "delta")]
    fn test_deltalake_benchmark() {
        let delta = SaveToDelta::new(function_name!().into()).expect("ok");
        writer_benchmark(&mut delta.writer());
    }

    fn do_read_10(save: &mut impl StoreReader) {
        for _ in 0..10 {
            let it = save.load(OrderBy::FsCreateTime, 0).expect("read ok");
            for _ in it {}
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
    #[cfg(feature = "delta")]
    fn test_deltalake_rw_benchmark() {
        let save_to_delta = SaveToDelta::new(function_name!()).expect("deltalake create");
        multi_read_single_writer_benchmark(save_to_delta);
    }
}
