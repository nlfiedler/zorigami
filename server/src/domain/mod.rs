//
// Copyright (c) 2020 Nathan Fiedler
//
use log::error;
use rusty_ulid::generate_ulid_string;

pub mod entities;
pub mod managers;
pub mod repositories;
pub mod usecases;

///
/// Return the unique bucket name for this computer and user.
///
pub fn computer_bucket_name(unique_id: &str) -> String {
    match blob_uuid::to_uuid(unique_id) {
        Ok(uuid) => uuid.to_simple().to_string(),
        Err(err) => {
            error!("failed to convert unique ID: {:?}", err);
            generate_ulid_string().to_lowercase()
        }
    }
}
