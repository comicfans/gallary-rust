use std::{path::PathBuf, sync::Arc};

#[macro_use]
use rocket::response::stream::TextStream;
use rocket::{State, get, request::FromParam};

extern crate rocket;

pub struct ServerConfig {
    pub store_path: Arc<PathBuf>,
}

use crate::common::OrderBy;
use crate::sqlite::SaveToSqlite;

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
pub fn list(
    server_config: &State<ServerConfig>,
    order_by: OrderBy,
    limit: usize,
) -> TextStream![String] {
    //return "abc".to_owned();
    TextStream! {
        yield "abc".to_owned();
        yield "bbc".to_owned();
    }
}
