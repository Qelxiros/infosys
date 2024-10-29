mod bb;
mod db;
mod util;

extern crate config;
extern crate sqlx;

use db::db_init;
use db::retrieve_strings_for_message_id;

use util::convert_to_vec8;
use util::get_naivetime_now;
use util::tuple_to_bytestring;

use bb::END_PACKET;
use bb::START_PACKET;

use std::fs::File;
use std::io::Write;

fn main() {
    let settings = config::Config::default()
        .merge(config::File::with_name("settings"))
        .unwrap();
    let mut con = db_init(&settings);
    let now = get_naivetime_now();

    let mut sign_input: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    // Get the most recent message to present

    for row in futures::executor::block_on(
        sqlx::query!(
            "SELECT message_id FROM schedule
        WHERE timeslot<=$1
        ORDER BY timeslot
        DESC LIMIT 1",
            now
        )
        .fetch_all(&mut con),
    )
    .unwrap()
    {
        let rowid: i32 = row.message_id;
        let result: Vec<(String, String)> = retrieve_strings_for_message_id(&mut con, rowid);
        println!("Message Id: {}, Contents:{:?}", rowid, result);
        for res in result {
            sign_input.push(tuple_to_bytestring(res));
        }
    }

    // Get this shit out onto the sign
    let mut file = match File::create(settings.get_str("sign_path").unwrap()) {
        Err(why) => panic!("couldn't create : {why}\nDo you not have permissions?",),
        Ok(file) => file,
    };

    file.write_all(START_PACKET).unwrap();
    file.write_all(convert_to_vec8(sign_input).as_slice())
        .unwrap();
    file.write_all(END_PACKET).unwrap();
    file.flush().unwrap();
}
