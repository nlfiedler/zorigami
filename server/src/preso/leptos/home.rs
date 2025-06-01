//
// Copyright (c) 2025 Nathan Fiedler
//
use crate::preso::leptos::nav;
use leptos::prelude::*;

#[component]
pub fn HomePage() -> impl IntoView {
    view! {
        <nav::NavBar />
        <section class="section">
            <h1 class="title">Zorigami</h1>
            <h2 class="subtitle">To be implemented</h2>
            <div class="content">
                <p>Come back again soon.</p>
            </div>
        </section>
    }
}
