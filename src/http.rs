use std::{path::PathBuf, sync::Arc};

use rocket::response::stream::TextStream;
use rocket::{State, get, request::FromParam};

extern crate rocket;

pub struct ServerConfig {
    pub store_path: Arc<PathBuf>,
}

use crate::common::{BasicPicture, OrderBy, StoreReader};
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

#[get("/<order_by>/<limit>")]
//#[get("/<limit>")]
pub async fn list(
    server_config: &State<ServerConfig>,
    order_by: OrderBy,
    limit: usize,
) -> TextStream![String] {
    let mut read = SqliteReader::new(server_config.store_path.to_path_buf()).expect("ok");
    TextStream! {

        yield "[\n".to_owned();
        let res = read.load(order_by, limit).expect("ok");

        let mut first = true;
        for v in res {

            if !first{
                yield ",\n".to_owned();
            }else{
                first = false;
            }

            let j = serde_json::to_string(&v).expect("json");
            yield j;
        }
        yield "\n]\n".to_owned();
    }
}
