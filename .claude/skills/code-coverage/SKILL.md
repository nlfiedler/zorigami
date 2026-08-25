---
name: code-coverage
description: Generate an HTML code coverage report for the Rust workspace using grcov. Use when the user asks to measure, check, or generate test/code coverage.
---

# Code Coverage

```bash
cargo install grcov
rustup component add llvm-tools
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="zorigami-%p-%m.profraw"
cargo clean && cargo build && cargo test
grcov . -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o ./target/debug/coverage/
```

Open `./target/debug/coverage/index.html` to view the report.
