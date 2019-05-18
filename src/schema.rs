//
// Copyright (c) 2019 Nathan Fiedler
//
use juniper::{
    graphql_object, graphql_scalar, FieldResult, GraphQLEnum, GraphQLObject, ParseScalarResult,
    ParseScalarValue, RootNode, Value,
};

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
    #[graphql(description = "Encoded set of options for this store.")]
    options: String,
}

pub struct QueryRoot;

graphql_object!(QueryRoot: () |&self| {
    field store(&executor, key: String) -> FieldResult<Store> {
        Ok(Store{
            key: "1234".to_owned(),
            options: "{json}".to_owned(),
        })
    }
});

// #[derive(GraphQLInputObject)]
// #[graphql(description = "A humanoid creature in the Star Wars universe")]
// struct NewHuman {
//     name: String,
//     appears_in: Vec<Episode>,
//     home_planet: String,
// }

pub struct MutationRoot;

graphql_object!(
    MutationRoot: () | &self | {
        // field createHuman(&executor, new_human: NewHuman) -> FieldResult<Human> {
        //     Ok(Human{
        //         id: "1234".to_owned(),
        //         name: new_human.name,
        //         appears_in: new_human.appears_in,
        //         home_planet: new_human.home_planet,
        //     })
        // }
    }
);

pub type Schema = RootNode<'static, QueryRoot, MutationRoot>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {})
}
