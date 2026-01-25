//
// Copyright (c) 2024 Nathan Fiedler
//

/// Retrieve the user-defined passphrase.
///
/// Returns a default if one has not been defined.
pub fn get_passphrase() -> String {
    std::env::var("PASSPHRASE").unwrap_or_else(|_| "keyboard cat".to_owned())
}
