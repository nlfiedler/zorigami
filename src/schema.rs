//
// Copyright (c) 2019 Nathan Fiedler
//
use juniper::{
    graphql_object, graphql_scalar, FieldResult, GraphQLEnum, GraphQLObject, ParseScalarResult,
    ParseScalarValue, RootNode, Value,
};
use std::str::FromStr;
use zorigami::database::Database;
use zorigami::store;

// Our GraphQL version of the core::Checksum type. It is tedious to implement
// all of the juniper interfaces, and the macro requires having a `from_str`
// where our type already has that method. This just seemed easier...
struct Digest(String);

// need `where Scalar = <S>` parameterization to use this with objects
// c.f. https://github.com/graphql-rust/juniper/issues/358 for details
graphql_scalar!(Digest where Scalar = <S> {
    description: "A SHA1 or SHA256 checksum, with algorithm prefix."

    resolve(&self) -> Value {
        Value::scalar(self.0.clone())
    }

    from_input_value(v: &InputValue) -> Option<Digest> {
        v.as_scalar_value::<String>().filter(|s| {
            // make sure the input value actually looks like a digest
            s.starts_with("sha1-") || s.starts_with("sha256-")
        }).map(|s| Digest(s.to_owned()))
    }

    from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
});

// Define a larger integer type so we can represent those larger values, such as
// file sizes and epoch time in milliseconds. Some of the core types define
// properties that are unsigned 32-bit integers, so to be certain we can
// represent those values in GraphQL, we will use this type.
struct BigInt(i64);

// need `where Scalar = <S>` parameterization to use this with objects
// c.f. https://github.com/graphql-rust/juniper/issues/358 for details
graphql_scalar!(BigInt where Scalar = <S> {
    description: "An integer type larger than the standard signed 32-bit."

    resolve(&self) -> Value {
        Value::scalar(format!("{}", self.0))
    }

    from_input_value(v: &InputValue) -> Option<BigInt> {
        v.as_scalar_value::<String>().filter(|s| {
            // make sure the input value parses as an integer
            i64::from_str_radix(s, 10).is_ok()
        }).map(|s| BigInt(i64::from_str_radix(s, 10).unwrap()))
    }

    from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
        <String as ParseScalarValue<S>>::from_str(value)
    }
});

#[derive(GraphQLEnum)]
#[graphql(description = "Type of the entry in the tree.")]
enum EntryType {
    File,
    Directory,
    SymLink,
}

#[derive(GraphQLObject)]
#[graphql(description = "A file, directory, or link within a tree.")]
struct TreeEntry {
    #[graphql(description = "File system name of this entry.")]
    name: String,
    #[graphql(description = "File system type of this entry.")]
    fstype: EntryType,
    #[graphql(description = "Coordinates for this entry in the database.")]
    reference: String,
}

#[derive(GraphQLObject)]
#[graphql(description = "A collection of files, directories, and links.")]
struct Tree {
    entries: Vec<TreeEntry>,
}

#[derive(GraphQLObject)]
#[graphql(description = "A single backup.")]
struct Snapshot {
    #[graphql(description = "The snapshot before this one, if any.")]
    parent: Option<Digest>,
    #[graphql(description = "Time when the snapshot was first created.")]
    start_time: BigInt,
    #[graphql(description = "Time when the snapshot completely finished.")]
    end_time: Option<BigInt>,
    #[graphql(description = "Total number of files contained in this snapshot.")]
    file_count: BigInt,
    #[graphql(description = "Reference to the tree containing all of the files.")]
    tree: Digest,
}

#[derive(GraphQLObject)]
#[graphql(description = "A single version of a saved file.")]
struct File {
    #[graphql(description = "Reference to the file in the database.")]
    digest: Digest,
    #[graphql(description = "Byte length of this version of the file.")]
    length: BigInt,
}

#[derive(GraphQLObject)]
#[graphql(description = "The directory structure which will be saved.")]
struct Dataset {
    #[graphql(description = "Opaque identifier for this dataset.")]
    key: String,
    #[graphql(description = "Unique computer identifier.")]
    computer_id: String,
    #[graphql(description = "Path that is being backed up.")]
    basepath: String,
    #[graphql(description = "Reference to most recent snapshot.")]
    latest_snapshot: Option<Digest>,
    #[graphql(description = "Path to temporary workspace for backup process.")]
    workspace: String,
    #[graphql(description = "Desired byte length of pack files.")]
    pack_size: BigInt,
    #[graphql(description = "Identifiers of stores used for saving packs.")]
    stores: Vec<String>,
}

#[derive(GraphQLObject)]
#[graphql(description = "Local or remote store for pack files.")]
struct Store {
    #[graphql(description = "Opaque identifier of this store.")]
    key: String,
    #[graphql(description = "Base64 encoded JSON of store options.")]
    options: String,
}

pub struct QueryRoot;

graphql_object!(QueryRoot: Database |&self| {
    #[doc = "Find all named store configurations."]
    field stores(&executor) -> FieldResult<Vec<String>> {
        let database = executor.context();
        let stores = store::find_stores(database)?;
        Ok(stores)
    }

    #[doc = "Retrieve the named store configuration."]
    field store(&executor, key: String) -> FieldResult<Store> {
        let database = executor.context();
        let stor = store::load_store(database, &key)?;
        let json: String = stor.get_config().to_json()?;
        let encoded = base64::encode(&json);
        Ok(Store{
            key,
            options: encoded,
        })
    }
});

pub struct MutationRoot;

graphql_object!(MutationRoot: Database | &self | {
    #[doc = "Define a new store with the given configuration."]
    field defineStore(&executor, type_name: String, options: String) -> FieldResult<Store> {
        let database = executor.context();
        let decoded = base64::decode(&options)?;
        let json = std::str::from_utf8(&decoded)?;
        let store_type = store::StoreType::from_str(&type_name)?;
        let mut stor = store::build_store(store_type, None);
        stor.get_config_mut().from_json(&json)?;
        store::save_store(&database, stor.as_ref())?;
        let key = store::store_name(stor.as_ref());
        Ok(Store{
            key,
            options,
        })
    }

    #[doc = "Update the saved store configuration."]
    field updateStore(&executor, key: String, options: String) -> FieldResult<Store> {
        let database = executor.context();
        let decoded = base64::decode(&options)?;
        let json = std::str::from_utf8(&decoded)?;
        let mut stor = store::load_store(database, &key)?;
        stor.get_config_mut().from_json(&json)?;
        store::save_store(&database, stor.as_ref())?;
        Ok(Store{
            key,
            options,
        })
    }
});

pub type Schema = RootNode<'static, QueryRoot, MutationRoot>;

/// Create the GraphQL schema.
pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use failure::Error;
    use juniper::{InputValue, Variables};
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_store_access() -> Result<(), Error> {
        let db_path = "tmp/test/schema/stores/rocksdb";
        let _ = fs::remove_dir_all(db_path);
        let ctx = Database::new(Path::new(db_path))?;
        let schema = create_schema();

        // make sure there are no stores in the database
        let (res, _errors) = juniper::execute(
            r#"query { stores }"#,
            None,
            &schema,
            &Variables::new(),
            &ctx,
        )
        .unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("stores").unwrap();
        let res = res.as_list_value().unwrap();
        assert!(res.is_empty());

        // query for a store that does not exist, should return one
        // with default settings
        let (res, _errors) = juniper::execute(
            r#"query { store(key: "store/local/foobar") { options } }"#,
            None,
            &schema,
            &Variables::new(),
            &ctx,
        )
        .unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("store").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("options").unwrap();
        let res = res.as_scalar_value::<String>().unwrap();
        let decoded = base64::decode(&res)?;
        let json = std::str::from_utf8(&decoded)?;
        assert!(json.contains("basepath"));

        // define a new local store with some options
        let mut vars = Variables::new();
        let options = base64::encode(r#"{"basepath": "/some/local/path"}"#);
        vars.insert("options".to_owned(), InputValue::scalar(options));
        let (res, _errors) = juniper::execute(
            r#"mutation DefineStore($options: String!) {
                defineStore(typeName: "local", options: $options) {
                    key
                }
            }"#,
            Some("DefineStore"),
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("defineStore").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("key").unwrap();
        let key = res.as_scalar_value::<String>().unwrap();
        assert!(key.starts_with("store/local/"));

        // call stores query to ensure the new local store is returned
        let (res, _errors) = juniper::execute(
            r#"query { stores }"#,
            None,
            &schema,
            &Variables::new(),
            &ctx,
        )
        .unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("stores").unwrap();
        let res = res.as_list_value().unwrap();
        assert_eq!(res.len(), 1);
        let value = Value::scalar::<String>(key.to_owned());
        assert!(res.contains(&value));

        // fetch the local store to make sure the options were saved
        let query = format!(r#"query {{ store(key: "{}") {{ options }} }}"#, key);
        let (res, _errors) =
            juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("store").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("options").unwrap();
        let res = res.as_scalar_value::<String>().unwrap();
        let decoded = base64::decode(&res)?;
        let json = std::str::from_utf8(&decoded)?;
        assert!(json.contains("/some/local/path"));

        // update the store configuration to something different
        let mut vars = Variables::new();
        vars.insert("key".to_owned(), InputValue::scalar(key.to_owned()));
        let options = base64::encode(r#"{"basepath": "/totally/different"}"#);
        vars.insert("options".to_owned(), InputValue::scalar(options));
        let (res, _errors) = juniper::execute(
            r#"mutation UpdateStore($key: String!, $options: String!) {
                updateStore(key: $key, options: $options) {
                    key
                }
            }"#,
            Some("UpdateStore"),
            &schema,
            &vars,
            &ctx,
        )
        .unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("updateStore").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("key").unwrap();
        let key = res.as_scalar_value::<String>().unwrap();
        assert!(key.starts_with("store/local/"));

        // fetch the local store to make sure the options were saved
        let query = format!(r#"query {{ store(key: "{}") {{ options }} }}"#, key);
        let (res, _errors) =
            juniper::execute(&query, None, &schema, &Variables::new(), &ctx).unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("store").unwrap();
        let res = res.as_object_value().unwrap();
        let res = res.get_field_value("options").unwrap();
        let res = res.as_scalar_value::<String>().unwrap();
        let decoded = base64::decode(&res)?;
        let json = std::str::from_utf8(&decoded)?;
        assert!(json.contains("/totally/different"));
        Ok(())
    }
}
