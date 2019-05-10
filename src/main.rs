//
// Copyright (c) 2019 Nathan Fiedler
//
use juniper_warp;
use log::info;
use pretty_env_logger;
use warp::{self, Filter};

mod schema;

pub fn main() {
    pretty_env_logger::init();
    let schema = schema::create_schema();
    let state = warp::any().map(move || ());
    let graphql_filter = juniper_warp::make_graphql_filter(schema, state.boxed());
    info!("listening on http://localhost:8080/...");
    warp::serve(
        warp::get2()
            .and(warp::path("graphiql"))
            .and(juniper_warp::graphiql_filter("/graphql"))
            .or(warp::fs::dir("public/"))
            .or(warp::path("graphql").and(graphql_filter)),
    )
    .run(([127, 0, 0, 1], 8080));
}
