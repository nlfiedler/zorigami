//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use crate::domain::services::buckets::{
    BucketNameGenerator, BucketNamingPolicy, BucketNamingPolicyResolver,
};
use anyhow::{Error, anyhow};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Generate a suitable bucket name using the current time and random bytes.
///
/// Ideally the name should conform to all of the requirements of all of the
/// supported cloud services, although the pack sources are allowed to fix the
/// names if there is a problem.
///
/// In short, the result will be fairly long but less than 63 characters, does
/// not include any prohibited characters, and will be lowercase.
///
/// The value is inspired by ULID and as such the first 48 bits are the
/// milliseconds since the epoch plus 256 bits of randomness, then base32hex
/// encoded so that the generated names sort lexicographically.
pub(crate) fn generate_bucket_name() -> String {
    use rand::RngExt;

    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_millis() as u64;
    let mut rando = [0u8; 32];
    rand::rng().fill(&mut rando);
    let mut result = [0u8; 38];
    // Copy low 6 bytes (48 bits) of time into the high bytes of result, then
    // copy all 32 bytes of rando into the lower bytes of result, and finally
    // base32hex encode and lowercase.
    let ms_bytes = ms.to_be_bytes();
    result[..6].copy_from_slice(&ms_bytes[2..]);
    result[6..].copy_from_slice(&rando);
    data_encoding::BASE32HEX_NOPAD
        .encode(&result)
        .to_lowercase()
}

/// Decode the 48-bit millisecond timestamp embedded at the start of a bucket
/// name produced by [`generate_bucket_name`].
fn decode_bucket_timestamp_ms(name: &str) -> Result<u64, Error> {
    let decoded = data_encoding::BASE32HEX_NOPAD
        .decode(name.to_uppercase().as_bytes())
        .map_err(Error::from)?;
    if decoded.len() < 6 {
        return Err(anyhow!("decoded bucket name is shorter than 6 bytes"));
    }
    let mut be = [0u8; 8];
    be[2..].copy_from_slice(&decoded[..6]);
    Ok(u64::from_be_bytes(be))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_millis() as u64
}

fn days_to_ms(days: usize) -> u64 {
    (days as u64).saturating_mul(24 * 60 * 60 * 1000)
}

/// Generator that emits up to `limit` distinct bucket names and then selects a
/// bucket at random from the pool for every subsequent request.
pub struct RandomPoolGenerator {
    repo: Arc<dyn RecordRepository>,
    limit: usize,
}

impl RandomPoolGenerator {
    pub fn new(repo: Arc<dyn RecordRepository>, limit: usize) -> Self {
        Self { repo, limit }
    }

    fn generate_and_record(&self) -> Result<String, Error> {
        let name = generate_bucket_name();
        self.repo.add_bucket(&name)?;
        Ok(name)
    }

    fn pick(&self) -> Result<String, Error> {
        let count = self.repo.count_buckets()?;
        if count < self.limit {
            self.generate_and_record()
        } else {
            self.repo
                .get_random_bucket()?
                .ok_or_else(|| anyhow!("no buckets available in pool"))
        }
    }
}

impl BucketNameGenerator for RandomPoolGenerator {
    fn generate_name(&self) -> String {
        self.pick().unwrap_or_else(|_| generate_bucket_name())
    }

    fn generate_new_name(&self) -> String {
        self.generate_and_record()
            .unwrap_or_else(|_| generate_bucket_name())
    }
}

/// Generator that reuses the most recent bucket name for `days` days before
/// rolling over to a new one.
pub struct ScheduledGenerator {
    repo: Arc<dyn RecordRepository>,
    days: usize,
}

impl ScheduledGenerator {
    pub fn new(repo: Arc<dyn RecordRepository>, days: usize) -> Self {
        Self { repo, days }
    }

    fn generate_and_record(&self) -> Result<String, Error> {
        let name = generate_bucket_name();
        self.repo.add_bucket(&name)?;
        Ok(name)
    }

    fn pick(&self) -> Result<String, Error> {
        if let Some(last) = self.repo.get_last_bucket()?
            && let Ok(created_ms) = decode_bucket_timestamp_ms(&last)
            && now_ms().saturating_sub(created_ms) < days_to_ms(self.days)
        {
            return Ok(last);
        }
        self.generate_and_record()
    }
}

impl BucketNameGenerator for ScheduledGenerator {
    fn generate_name(&self) -> String {
        self.pick().unwrap_or_else(|_| generate_bucket_name())
    }

    fn generate_new_name(&self) -> String {
        self.generate_and_record()
            .unwrap_or_else(|_| generate_bucket_name())
    }
}

/// Generator that combines the scheduled and random-pool policies: while the
/// bucket pool is below `limit`, a new bucket is minted only once every `days`
/// days; once the pool is full, names are chosen at random.
pub struct ScheduledRandomPoolGenerator {
    repo: Arc<dyn RecordRepository>,
    days: usize,
    limit: usize,
}

impl ScheduledRandomPoolGenerator {
    pub fn new(repo: Arc<dyn RecordRepository>, days: usize, limit: usize) -> Self {
        Self { repo, days, limit }
    }

    fn generate_and_record(&self) -> Result<String, Error> {
        let name = generate_bucket_name();
        self.repo.add_bucket(&name)?;
        Ok(name)
    }

    fn pick(&self) -> Result<String, Error> {
        let count = self.repo.count_buckets()?;
        if count < self.limit {
            if let Some(last) = self.repo.get_last_bucket()?
                && let Ok(created_ms) = decode_bucket_timestamp_ms(&last)
                && now_ms().saturating_sub(created_ms) < days_to_ms(self.days)
            {
                return Ok(last);
            }
            self.generate_and_record()
        } else {
            self.repo
                .get_random_bucket()?
                .ok_or_else(|| anyhow!("no buckets available in pool"))
        }
    }
}

impl BucketNameGenerator for ScheduledRandomPoolGenerator {
    fn generate_name(&self) -> String {
        self.pick().unwrap_or_else(|_| generate_bucket_name())
    }

    fn generate_new_name(&self) -> String {
        self.generate_and_record()
            .unwrap_or_else(|_| generate_bucket_name())
    }
}

/// Resolver that maps a [`BucketNamingPolicy`] to the appropriate
/// [`BucketNameGenerator`] implementation, cloning a repository handle for the
/// generator to own.
pub struct BucketNamingPolicyResolverImpl {
    repo: Arc<dyn RecordRepository>,
}

impl BucketNamingPolicyResolverImpl {
    pub fn new(repo: Arc<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl BucketNamingPolicyResolver for BucketNamingPolicyResolverImpl {
    fn build_generator(
        &self,
        policy: BucketNamingPolicy,
    ) -> Result<Box<dyn BucketNameGenerator>, Error> {
        let repo = Arc::clone(&self.repo);
        Ok(match policy {
            BucketNamingPolicy::RandomPool(limit) => {
                Box::new(RandomPoolGenerator::new(repo, limit))
            }
            BucketNamingPolicy::Scheduled(days) => Box::new(ScheduledGenerator::new(repo, days)),
            BucketNamingPolicy::ScheduledRandomPool { days, limit } => {
                Box::new(ScheduledRandomPoolGenerator::new(repo, days, limit))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::repositories::MockRecordRepository;

    #[test]
    fn test_generate_bucket_name() {
        let bucket = generate_bucket_name();
        assert_eq!(bucket.len(), 61, "bucket name is 61 characters");
        for c in bucket.chars() {
            assert!(c.is_ascii_alphanumeric());
            if c.is_ascii_alphabetic() {
                assert!(c.is_ascii_lowercase());
            }
        }
        let second = generate_bucket_name();
        assert_ne!(bucket, second);
    }

    #[test]
    fn test_decode_bucket_timestamp_roundtrip() {
        let name = generate_bucket_name();
        let ms = decode_bucket_timestamp_ms(&name).expect("decode");
        let now = now_ms();
        assert!(ms <= now);
        assert!(now - ms < 5_000);
    }

    #[test]
    fn test_random_pool_below_limit_generates() {
        let mut mock = MockRecordRepository::new();
        mock.expect_count_buckets().returning(|| Ok(2));
        mock.expect_add_bucket().times(1).returning(|_| Ok(()));
        let generator = RandomPoolGenerator::new(Arc::new(mock), 5);
        let name = generator.generate_name();
        assert_eq!(name.len(), 61);
    }

    #[test]
    fn test_random_pool_at_limit_picks_random() {
        let mut mock = MockRecordRepository::new();
        mock.expect_count_buckets().returning(|| Ok(5));
        mock.expect_add_bucket().times(0).returning(|_| Ok(()));
        mock.expect_get_random_bucket()
            .returning(|| Ok(Some("existing_bucket".to_owned())));
        let generator = RandomPoolGenerator::new(Arc::new(mock), 5);
        assert_eq!(generator.generate_name(), "existing_bucket");
    }

    #[test]
    fn test_random_pool_generate_new_always_creates() {
        let mut mock = MockRecordRepository::new();
        mock.expect_add_bucket().times(1).returning(|_| Ok(()));
        let generator = RandomPoolGenerator::new(Arc::new(mock), 5);
        let name = generator.generate_new_name();
        assert_eq!(name.len(), 61);
    }

    #[test]
    fn test_scheduled_reuses_recent_bucket() {
        let recent = generate_bucket_name();
        let reused = recent.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_last_bucket()
            .returning(move || Ok(Some(reused.clone())));
        mock.expect_add_bucket().times(0).returning(|_| Ok(()));
        let generator = ScheduledGenerator::new(Arc::new(mock), 7);
        assert_eq!(generator.generate_name(), recent);
    }

    #[test]
    fn test_scheduled_rolls_over_when_stale() {
        // Construct a bucket name whose embedded timestamp is zero (epoch) so
        // that the computed age is always far beyond any realistic `days`.
        let stale = {
            let result = [0u8; 38];
            data_encoding::BASE32HEX_NOPAD
                .encode(&result)
                .to_lowercase()
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_last_bucket()
            .returning(move || Ok(Some(stale.clone())));
        mock.expect_add_bucket().times(1).returning(|_| Ok(()));
        let generator = ScheduledGenerator::new(Arc::new(mock), 7);
        let name = generator.generate_name();
        assert_eq!(name.len(), 61);
    }

    #[test]
    fn test_scheduled_generates_when_no_last_bucket() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_last_bucket().returning(|| Ok(None));
        mock.expect_add_bucket().times(1).returning(|_| Ok(()));
        let generator = ScheduledGenerator::new(Arc::new(mock), 7);
        let name = generator.generate_name();
        assert_eq!(name.len(), 61);
    }

    #[test]
    fn test_scheduled_random_pool_reuses_recent_while_below_limit() {
        let recent = generate_bucket_name();
        let reused = recent.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_count_buckets().returning(|| Ok(2));
        mock.expect_get_last_bucket()
            .returning(move || Ok(Some(reused.clone())));
        mock.expect_add_bucket().times(0).returning(|_| Ok(()));
        let generator = ScheduledRandomPoolGenerator::new(Arc::new(mock), 7, 5);
        assert_eq!(generator.generate_name(), recent);
    }

    #[test]
    fn test_scheduled_random_pool_rolls_over_below_limit_when_stale() {
        let stale = {
            let result = [0u8; 38];
            data_encoding::BASE32HEX_NOPAD
                .encode(&result)
                .to_lowercase()
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_count_buckets().returning(|| Ok(2));
        mock.expect_get_last_bucket()
            .returning(move || Ok(Some(stale.clone())));
        mock.expect_add_bucket().times(1).returning(|_| Ok(()));
        let generator = ScheduledRandomPoolGenerator::new(Arc::new(mock), 7, 5);
        let name = generator.generate_name();
        assert_eq!(name.len(), 61);
    }

    #[test]
    fn test_scheduled_random_pool_at_limit_picks_random() {
        let mut mock = MockRecordRepository::new();
        mock.expect_count_buckets().returning(|| Ok(5));
        mock.expect_add_bucket().times(0).returning(|_| Ok(()));
        mock.expect_get_random_bucket()
            .returning(|| Ok(Some("existing_bucket".to_owned())));
        let generator = ScheduledRandomPoolGenerator::new(Arc::new(mock), 7, 5);
        assert_eq!(generator.generate_name(), "existing_bucket");
    }
}
