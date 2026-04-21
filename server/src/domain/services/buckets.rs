//
// Copyright (c) 2026 Nathan Fiedler
//
use anyhow::Error;
#[cfg(test)]
use mockall::automock;

/// The policy for generating bucket names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BucketNamingPolicy {
    /// Generate up to N buckets then select randomly thereafter.
    RandomPool(usize),
    /// Generate a new bucket every N days.
    Scheduled(usize),
    /// Every `days` produce a new bucket up to `limit` buckets then select
    /// randomly thereafter.
    ScheduledRandomPool { days: usize, limit: usize },
}

/// A generator of bucket names that applies a specific policy.
#[cfg_attr(test, automock)]
pub trait BucketNameGenerator: Send + Sync {
    /// Produce a bucket name according to the associated policy.
    ///
    /// Call this function for each pack file that is to be uploaded to ensure
    /// buckets are reused and yet not overused, as appropriate.
    fn generate_name(&self) -> String;

    /// Produce a different bucket name than the last call to [`generate_name`].
    ///
    /// Most pack stores deal with bucket collision internally so this function
    /// is of limited usefulness at this time.
    fn generate_new_name(&self) -> String;
}

/// Builder for bucket name generators.
#[cfg_attr(test, automock)]
pub trait BucketNamingPolicyResolver: Send + Sync {
    /// Construct bucket name generator for the given naming policy.
    fn build_generator(
        &self,
        policy: BucketNamingPolicy,
    ) -> Result<Box<dyn BucketNameGenerator>, Error>;
}
