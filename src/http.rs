#[macro_use]
use rocket::response::stream::TextStream;
extern crate rocket;
enum OrderBy {
    FsCreateTime,
    FsModifyTime,
    ExifCreateTime,
}

#[get("/list/<order_by>/<limit>")]
fn index() -> TextStream![&str] {
    "Hello, world!"
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![index])
}
