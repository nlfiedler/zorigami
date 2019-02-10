//
// Copyright (c) 2019 Nathan Fiedler
//
use uuid::Uuid;
use ulid::Ulid;

///
/// Generate a type 5 UUID based on the given values.
///
pub fn generate_unique_id(username: &str, hostname: &str) -> String {
    let mut name = String::from(username);
    name.push(':');
    name.push_str(hostname);
    let bytes = name.into_bytes();
    Uuid::new_v5(&Uuid::NAMESPACE_URL, &bytes).to_hyphenated().to_string()
}

///
/// Generate a suitable bucket name, using a ULID and the given UUID.
///
pub fn generate_bucket_name(unique_id: &str) -> String {
    let shorter = String::from(unique_id).replace("-", "");
    let mut ulid = Ulid::new().to_string();
    ulid.push_str(&shorter);
    ulid.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_unique_id() {
        let uuid = generate_unique_id("charlie", "localhost");
        assert_eq!(uuid, "747267d5-6e70-5711-8a9a-a40c24c1730f");
    }

    #[test]
    fn test_generate_bucket_name() {
        let uuid = generate_unique_id("charlie", "localhost");
        let bucket = generate_bucket_name(&uuid);
        // Ensure the generated name is safe for the "cloud", which so far means
        // Google Cloud Storage and Amazon Glacier. It needs to be reasonably
        // short, must consist only of lowercase letters or digits.
        assert_eq!(bucket.len(), 58, "bucket name is 58 characters");
        for c in bucket.chars() {
            assert!(c.is_ascii_alphanumeric());
            if c.is_ascii_alphabetic() {
                assert!(c.is_ascii_lowercase());
            }
        }
    }
}
