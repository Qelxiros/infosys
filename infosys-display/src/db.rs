extern crate config;
extern crate sqlx;

use self::sqlx::query;

use self::sqlx::{Connection, PgConnection};

static SQL_CREATE_SCHEDULE: &'static str = "CREATE TABLE schedule (
    timeslot        TIME NOT NULL PRIMARY KEY,
    message_id      INTEGER NOT NULL,
    FOREIGN KEY     (message_id) REFERENCES messages(id)
);";

static SQL_CREATE_MESSAGE: &'static str = "CREATE TABLE messages (
    id SERIAL PRIMARY KEY
);";

static SQL_CREATE_STRING: &'static str = "CREATE TABLE strings (
    id              INTEGER NOT NULL,
    message_id      INTEGER NOT NULL,
    mode            VARCHAR(32) NOT NULL,
    data            TEXT NOT NULL,
    PRIMARY KEY     (id, message_id),
    FOREIGN KEY     (message_id) REFERENCES messages(id)
);";

static SQL_DEFAULT_VALUES: &'static str = "
INSERT INTO messages (id) VALUES (0);

INSERT INTO schedule (timeslot, message_id) VALUES ('00:00:00', 0);

INSERT INTO strings (id, message_id, mode, data) VALUES (1, 0, 'STANDARD_HOLD', 'Welcome to CSH!');";

pub fn db_init(settings: &config::Config) -> PgConnection {
    let mut con: PgConnection =
        futures::executor::block_on(Connection::connect(&settings.get_str("dbstring").unwrap()))
            .unwrap();

    let mut init = true;
    init = init && check_or_create_db(&mut con, "messages", SQL_CREATE_MESSAGE);
    init = init && check_or_create_db(&mut con, "schedule", SQL_CREATE_SCHEDULE);
    init = init && check_or_create_db(&mut con, "strings", SQL_CREATE_STRING);

    // If we're doing a clean init add some default values!
    if init {
        futures::executor::block_on(sqlx::query(SQL_DEFAULT_VALUES).execute(&mut con)).unwrap();
    }
    return con;
}

fn check_or_create_db(con: &mut PgConnection, name: &str, sql: &str) -> bool {
    let exists: bool = futures::executor::block_on(
        sqlx::query(
            "SELECT 1::integer FROM pg_tables
            WHERE schemaname = 'public' AND tablename = $1::text;",
        )
        .bind(name)
        .fetch_optional(&mut *con),
    )
    .unwrap()
    .is_some();

    // Create the table!
    if !exists {
        futures::executor::block_on(sqlx::query(&sql).execute(con)).unwrap();
    }

    // Return true if we created the table!
    return !exists;
}

// TODO move this into a single request utilizing the schedule and foreign keys
pub fn retrieve_strings_for_message_id(con: &mut PgConnection, id: i32) -> Vec<(String, String)> {
    let mut str_list: Vec<(String, String)> = Vec::new();

    let results = futures::executor::block_on(
        query!(
            "SELECT id, message_id, data, mode FROM strings where message_id = $1",
            id
        )
        .fetch_all(con),
    )
    .unwrap();

    for string in results {
        str_list.push((string.mode, string.data));
    }
    return str_list;
}
