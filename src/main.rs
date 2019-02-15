//
// Copyright (c) 2019 Nathan Fiedler
//
#[macro_use]
extern crate juniper;
extern crate juniper_warp;
extern crate pretty_env_logger;
#[macro_use]
extern crate log;
extern crate warp;
use juniper::{EmptyMutation, FieldResult};
use warp::Filter;

struct Context;

// To make our context usable by Juniper, we have to implement a marker trait.
impl juniper::Context for Context {}

struct Query;

graphql_object!(Query: Context |&self| {

    /// Returns the version of the API as a string.
    field apiVersion() -> &str {
        "1.0"
    }

    /// Returns a greeting for the given name.
    field hello(&executor, name: String) -> FieldResult<String> {
        // Get the context from the executor.
        // let context = executor.context();
        // Get a db connection.
        // let connection = context.pool.get_connection()?;
        // Execute a db query.
        // Note the use of `?` to propagate errors.
        // let human = connection.find_human(&id)?;
        // Return the result.
        let mut result = String::from("Hello, ");
        result.push_str(&name);
        Ok(result)
    }
});

// A root schema consists of a query and a mutation.
// Request queries can be executed against a RootNode.
type Schema = juniper::RootNode<'static, Query, EmptyMutation<Context>>;

pub fn main() {
    pretty_env_logger::init();
    let schema = Schema::new(Query, EmptyMutation::new());
    let state = warp::any().map(move || Context {});
    let graphql_filter = juniper_warp::make_graphql_filter(schema, state.boxed());
    info!("listening on http://localhost:3030/...");
    warp::serve(
        warp::get2()
            .and(warp::path("graphiql"))
            .and(juniper_warp::graphiql_filter("/graphql"))
            .or(warp::fs::dir("public/"))
            .or(warp::path("graphql").and(graphql_filter)),
    )
    .run(([127, 0, 0, 1], 8080));
}
