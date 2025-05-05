use anyhow::Result;
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

pub struct MyDirEntry {
    pub path: PathBuf,
}

impl MyDirEntry {
    pub fn path(self: &Self) -> PathBuf {
        self.path.clone()
    }
}

pub trait FsOpCallback: Send {
    fn on_op(&mut self, path: MyDirEntry) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

use strum_macros::EnumString;
#[derive(EnumString)]
pub enum OrderBy {
    FsCreateTime,
    FsModifyTime,
    ExifCreateTime,
}

pub trait StoreReader: Send {
    fn load(
        &mut self,
        order_by: OrderBy,
        limit: usize,
        callback: &mut dyn FsOpCallback,
    ) -> Result<()>;
}

pub trait Store {
    fn reader(&self) -> impl StoreReader;
    fn writer(&self) -> impl FsOpCallback;
}

pub fn walk_files(root: &Path, callback: &mut dyn FsOpCallback) -> Result<()> {
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
