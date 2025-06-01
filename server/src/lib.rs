//
// Copyright (c) 2020 Nathan Fiedler
//
extern crate thiserror;

pub mod data;
pub mod domain;
pub mod preso;

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    use crate::preso::leptos::App;
    console_error_panic_hook::set_once();
    _ = console_log::init_with_level(log::Level::Debug);

    leptos::mount::hydrate_body(App);
}
