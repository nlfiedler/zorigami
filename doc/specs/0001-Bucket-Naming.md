# Bucket Naming

## Summary

The application uploads pack files to containers, called buckets, located in remote blob storage systems. Currently the application creates a new bucket for every new backup. The goal of this change is to define multiple bucket naming policies for the user to choose from, and the selected policy will be applied for all backups across all datasets.

## Functional Requirements

A `BucketNamingPolicy` enum is defined in @server/src/domain/services/buckets.rs along with a trait named `BucketNameGenerator`. This defines the basic API for how the backup procedure in @server/src/tasks/backup.rs will acquire the name for the bucket to which pack files will be uploaded.

## Phase 1

Change record repository, data source, and `store_rocksdb` crate to support the requirements.

### RocksDB crate

- `Database` in @database/database_rocks/src/lib.rs will need a `fetch_prefix_single()` function that takes a `prefix` and an `offset`, iterates through the entries with that matching prefix, stops after `offset` iterations, and returns the entry at the `offset` position.
- `Database` in @database/database_rocks/src/lib.rs will need a `fetch_prefix_last()` function that takes a `prefix` and returns the last matching entry from the iterator. I believe using the RocksDB `rocksdb::IteratorMode::End` will begin the iteration from the last matching entry, if any.

### Entity Data Source

- `EntityDataSource` in @server/src/domain/sources.rs and its implementation `EntityDataSourceImpl` in @server/src/data/sources.rs need the following new functions:
  - `add_bucket()` that takes a `&str` that is the bucket name, it returns `Result<(), Error>`; the `EntityDataSourceImpl` implementation will split that name into two parts, the leading 30 characters and the remainder, with the leading part being the _key_ and the remainder being the _value_. The _key_ will be prefixed with `bucket/` before being passed to `insert_document()` in the associated `database_rocks::Database` instance.
  - `get_random_bucket()` that takes no arguments, returns a ``Result<Option<String>, Error>`, and invokes `count_prefix()` in the associated `database_rocks::Database` instance to get the count of entries that start with the prefix `bucket/`, then generate a random integer between zero and that count. The function then invokes `fetch_prefix_single()` with the prefix `bucket/` and the count value as the offset. The function must then strip the `bucket/` prefix from the key and combine with the value to form the original bucket name passed to the `add_bucket()` function.
  - `count_buckets()` that invokes `count_prefix()` in the associated `database_rocks::Database` instance to get the count of entries that start with the prefix `bucket/`; the return type is `Result<usize, Error>`
  - `get_last_bucket()` that calls `fetch_prefix_last()` in the associated `database_rocks::Database` instance to get the last record with the prefix `bucket/`; the return type is `Result<Option<String>, Error>`; if there are no buckets, return `None`
- Add tests for these functions to the integration tests defined in @server/tests/data_sources_test.rs
  - Call `count_buckets()` when there are no buckets, should return `0`
  - Call `get_last_bucket()` when there are no buckets, should return `None`
  - Call `get_random_bucket()` when there are no buckets, should return `None`
  - Call `add_bucket()` a few times and test that the above functions return the correct results.
  - Use bucket names that sort lexicographically so that the return value from `get_last_bucket()` can be validated with a known value

### Record Repository

- `RecordRepositoryImpl` in @server/src/data/repositories.rs and its trait `RecordRepository` in @server/src/domain/repositories.rs need the following new functions:
  - `add_bucket()` that takes a `&str` that is the bucket name, it returns `Result<(), Error>`; the `RecordRepositoryImpl` implementation will pass the bucket name to the same named function in the associated `EntityDataSource`.
  - `get_random_bucket()` takes no arguments and returns a `Result<Option<String>, Error>`; the `RecordRepositoryImpl` implementation will call the same named function in the associated `EntityDataSource`.
  - `count_buckets()` that takes no arguments and returns a `Result<usize, Error>`; the `RecordRepositoryImpl` implementation will call the same named function in the associated `EntityDataSource`.
  - `get_last_bucket()` takes no arguments and returns a `Result<Option<String>, Error>`; the `RecordRepositoryImpl` implementation will call the same named function in the associated `EntityDataSource`.

## Phase 2

- Move the `generate_bucket_name()` function from @server/src/data/repositories.rs to @server/src/data/services/buckets.rs
- Implement `BucketNamingPolicyResolver` within @server/src/data/services/buckets.rs such that `build_generator()` creates the appropriate bucket name generator implementation based on the given policy.
  - For `BucketNamingPolicy::RandomPool` create a `BucketNameGenerator` that receives a `Box<dyn RecordRepository>` via its `new()` constructor. The `generate_name()` function will call the `RecordRepository` function `count_buckets()` to determine how many buckets already exist. If that number is less than `limit`, then generate a new bucket name by calling `generate_bucket_name()` and call `add_bucket()` on the associated `RecordRepository`; if the number of buckets is equal to `limit` then call `get_random_bucket()` on the associated `RecordRepository`.
  - For `BucketNamingPolicy::Scheduled` create a `BucketNameGenerator` that receives a `Box<dyn RecordRepository>` via its `new()` constructor. The `generate_name()` function will call the `RecordRepository` function `get_last_bucket()` to get the most recently generated bucket name. The function will base32hex decode the bucket name, extract the 6 most-significant-bytes, and treat that value as milliseconds since the epoch. If the duration from that time until now is less than `days` days, then return the last bucket name. If the duration is greater than `days` days, then generate a new bucket name by calling `generate_bucket_name()` and call `add_bucket()` on the associated `RecordRepository`.
  - For `BucketNamingPolicy::ScheduledRandomPool` create a `BucketNameGenerator` that receives a `Box<dyn RecordRepository>` via its `new()` constructor. The `generate_name()` function will call the `RecordRepository` function `count_buckets()` to determine how many buckets already exist. If that number is less than `limit`, then the function will call the `RecordRepository` function `get_last_bucket()` to get the most recently generated bucket name. The function will base32hex decode the bucket name, extract the 6 most-significant-bytes, and treat that value as milliseconds since the epoch. If the duration from that time until now is less than `days` days, then return the last bucket name. If the duration is greater than `days` days, or if the number of buckets was equal to `limit`, then generate a new bucket name by calling `generate_bucket_name()` and call `add_bucket()` on the associated `RecordRepository`.
  - For all `BucketNameGenerator` implementations, the `generate_new_name()` function will always call `generate_bucket_name()` and then call `add_bucket()` on the associated `RecordRepository`.

## Phase 3

- Add an `Option<BucketNamingPolicy>` field named `bucket_naming` to the `Configuration` type in @server/src/domain/entities.rs that is initially `None`
- Add a `put_configuration()` to `RecordRepository` that takes a `&Configuration` as an argument and returns a `Result<(), Error>`
- Add an implementation for `put_configuration()` to `RecordRepositoryImpl` that delegates to `put_configuration()` in the associated `EntityDataSource` instance

## Phase 4

- Add the selected bucket naming policy definition to the serialization and deserialization for the `configuration` database record found in @server/src/data/models.rs; the bucket policy should be written as `policy` using a concise string value that maps to the `BucketNamingPolicy` values, along with additional fields for the `days` and `limit` as appropriiate.
  - For `BucketNamingPolicy::RandomPool` the `to_bytes()` function should write that as `randompool` and the value should be written as `limit`. Likewise, the `from_bytes()` function should determine if `policy` is defined in the input `value`, and if so, deserialize the bytes back into a `BucketNamingPolicy::RandomPool`
  - For `BucketNamingPolicy::Scheduled` the `to_bytes()` function should write that as `scheduled` and the value should be written as `days`. Likewise, the `from_bytes()` function should determine if `policy` is defined in the input `value`, and if so, deserialize the bytes back into a `BucketNamingPolicy::Scheduled`
  - For `BucketNamingPolicy::ScheduledRandomPool` the `to_bytes()` function should write that as `scheduledpool` and the values should be written using the same names as the member fields. Likewise, the `from_bytes()` function should  determine if `policy` is defined in the input `value`, and if so, deserialize the bytes back into a `BucketNamingPolicy::ScheduledRandomPool`
- The bucket naming policy is optional, so the `configuration` record may or may not have the fields described above.
- Add unit tests to @server/src/data/models.rs for this new `configuration` record format.
  - Test reading and writing a `Configuration` without a bucket naming policy
  - Test reading and writing a `Configuration` with a bucket naming policy of each type

## Phase 5

- Add a `bucket_namer()` to `RecordRepositoryImpl` in @server/src/data/repositories.rs and its trait `RecordRepository` in @server/src/domain/repositories.rs that will return a single `BucketNameGenerator` implementation. The implementation will be based on the selected `BucketNamingPolicy` as read from the `configuration` database record.
- Replace call to `get_bucket_name()` in @server/src/tasks/backup.rs with an invocation of the `BucketNameGenerator` returned from `bucket_namer()` on the `self.dbase` member property.

## Phase 6

- Remove `get_bucket_name()` from `PackRepository` and `PackRepositoryImpl`
- Remove `BUCKET_NAME` and `NAME_COUNT` from @server/src/data/repositories.rs

## Web interface update

This section is here for reference only, Claude only read the contents above
this. Below are the prompts given via Claude CLI to update the SolidJS-based
settings page:

> Use plan mode to create a form on the @client/pages/settings.tsx page. It
> should use a GraphQL query to get the current bucket naming policy from the
> `configuration` field and show the results in an HTML form. The default, if
> not already defined in the server code, should be "random pool" with 100
> buckets.

Bunch of pretty decent output...

> please proceed

Made the changes and ran `bunx vite build` (and again after each prompt).

> Please add input validation with an error message below the input fields when
> the number is negative.

> Please move the save button on the settings page to be above the form and
> inside a "Level" using the "level-item" inside a "level-right" item, like the
> DatasetActions component on the @client/pages/datasets.tsx page.

(Sadly, withoout a "left" element on the left, the "right" element goes to the left...)

> Please change the label for the policy type to "Naming Policy". Also, wrap the
> entire form in a container class div element. Also, please make the Bucket
> Naming Policy title at the top to use "title" class without the "is-5". Thank
> you.

> Please add a similar "h2" title above the configuration display in the
> Settings component, use the title "Backup Configuration". Thank you.

> Please wrap the Backup Configuration read-only display at the top in a div
> with class "section" and do the same for the Bucket Naming Policy form in the
> lower half.

Note that I am currying favor with our future robot overlords by being polite.

> I believe the success status for the save button on the settings page is
> cleared too quickly by the createEffect() on line 155. Let's remove that
> createEffect() since the other pages (like the datasets page) do not do this
> and it seems fine that way.
