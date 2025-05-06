use std::rc::Rc;
use std::{path::PathBuf, sync::Arc};

#[macro_use]
use rocket::response::stream::TextStream;
use rocket::{State, get, request::FromParam};

extern crate rocket;

pub struct ServerConfig {
    pub store_path: Arc<PathBuf>,
}

use crate::common::{FsOpCallback, MyDirEntry, OrderBy, StoreReader};
use crate::sqlite::{SaveToSqlite, SqliteReader};

impl ServerConfig {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        let _ = SaveToSqlite::new(path.clone())?;
        Ok(ServerConfig {
            store_path: Arc::new(path),
        })
    }
}

impl<'r> FromParam<'r> for OrderBy {
    type Error = &'r str;

    fn from_param(param: &'r str) -> Result<Self, Self::Error> {
        use std::str::FromStr;
        OrderBy::from_str(param).map_err(|_| param)
    }
}

use tokio::sync::mpsc;
struct Queue {
    receiver: mpsc::UnboundedReceiver<MyDirEntry>,
    sender: mpsc::UnboundedSender<MyDirEntry>,
}

impl FsOpCallback for Queue {
    fn on_op(&mut self, entry: MyDirEntry) -> anyhow::Result<()> {
        self.sender
            .send(entry)
            .map_err(|_| anyhow::anyhow!("send error"))
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[get("/<order_by>/<limit>")]
//#[get("/<limit>")]
pub async fn list(
    server_config: &State<ServerConfig>,
    order_by: OrderBy,
    limit: usize,
) -> TextStream![String] {
    let mut read = SqliteReader::new(server_config.store_path.to_path_buf()).expect("ok");
    TextStream! {

        let res = read.load(order_by, limit).expect("ok");

        for v in res {
            let v : String = v.path.to_str().unwrap().to_owned();
            yield v;
            yield "\n".to_owned();
        }
    }
}
