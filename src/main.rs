//
// Copyright (c) 2019 Nathan Fiedler
//
use gotham::handler::assets::FileOptions;
use gotham::router::builder::{build_simple_router, DefineSingleRoute, DrawRoutes};

pub fn main() {
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
