use anyhow::Result;
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use serde::{Deserialize, Serialize};

use serde::ser::{SerializeStruct, Serializer};

pub struct Zoned(pub jiff::Zoned);

impl Serialize for Zoned {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("Zoned", 2)?;

        state.serialize_field("local_datetime", &self.0.timestamp())?;

        let tz_string = self.0.time_zone().iana_name().unwrap(); //TODO
        state.serialize_field("time_zone", &tz_string)?;
        //serializer.serialize_
        //state.serialize_field("timezone", &self.0.time_zone())?;
        state.end()
    }
}

#[derive(Serialize)]
pub struct BasicPicture {
    pub path: String,
    pub fs_create_time: Zoned,
    pub exif_create_time: Option<Zoned>,
}

pub struct PictureRecord {
    pub path: String,
    pub fs_create_time: Zoned,
}

impl PictureRecord {
    pub fn new(entry: &walkdir::DirEntry) -> anyhow::Result<PictureRecord> {
        let fs_create_time = jiff::Zoned::try_from(entry.metadata()?.created()?)?;
        let path = entry
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid utf8"))?;

        Ok(PictureRecord {
            path: path.to_owned(),
            fs_create_time: Zoned(fs_create_time),
        })
    }
}

pub trait FsOpCallback: Send {
    fn on_op(&mut self, picture_record: PictureRecord) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

use strum_macros::AsRefStr;

use strum_macros::EnumString;
#[derive(EnumString, AsRefStr)]
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
    ) -> Result<impl Iterator<Item = BasicPicture>>;
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
                if let Ok(record) = PictureRecord::new(&entry) {
                    let _ = callback.on_op(record);
                }
            }
            Err(_) => break,
        }
    }

    callback.flush()
}
