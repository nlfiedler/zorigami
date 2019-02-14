//
// Copyright (c) 2019 Nathan Fiedler
//
#[macro_use] extern crate juniper;
use {
    juniper::{
        FieldResult,
        Variables,
        EmptyMutation
    },
    gotham::handler::assets::FileOptions,
    gotham::router::builder::{
        build_simple_router,
        DefineSingleRoute,
        DrawRoutes
    }
};

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
    // GraphQL example
    let ctx = Context{};
    let (res, _errors) = juniper::execute(
        "query { hello(name: \"world\") }",
        None,
        &Schema::new(Query, EmptyMutation::new()),
        &Variables::new(),
        &ctx,
    ).unwrap();
    let result = res.as_object_value().unwrap();
    let field = result.get_field_value("hello").unwrap();
    let value: &str = field.as_scalar_value::<String>().map(|s| s as &str).unwrap();
    assert_eq!(value, "Hello, world");
    println!("{}", value);

    // Gotham example
    let path = "public";
    let addr = "127.0.0.1:7878";
    println!(
        "Listening for requests at http://{} from path {:?}",
        addr, &path
    );
    let router = build_simple_router(|route| {
        route.get("/").to_file("public/index.html");
        route.get("*").to_dir(
            FileOptions::new(&path)
                .with_gzip(true)
                .build(),
        );
    });
    gotham::start(addr, router)
}
