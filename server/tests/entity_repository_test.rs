//
// Copyright (c) 2024 Nathan Fiedler
//
use anyhow::Error;
use server::data::repositories::RecordRepositoryImpl;
use server::data::sources::EntityDataSourceImpl;
use server::domain::entities;
use server::domain::repositories::RecordRepository;
use server::domain::sources::EntityDataSource;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

// Test in which entity repository has a primary data source that is completely
// empty, and a non-empty fallback data source.
#[test]
fn test_full_fallback_empty_primary() -> Result<(), Error> {
    let db_base: PathBuf = ["tmp", "test", "database"].iter().collect();
    fs::create_dir_all(&db_base)?;
    let primary_path = tempfile::tempdir_in(&db_base)?;
    let primary = EntityDataSourceImpl::new(&primary_path).unwrap();
    let fallback_path = tempfile::tempdir_in(&db_base)?;
    let fallback = EntityDataSourceImpl::new(&fallback_path).unwrap();
    let mut repo = RecordRepositoryImpl::new(Arc::new(primary));

    // populate the data source with stores
    let mut properties: HashMap<String, String> = HashMap::new();
    properties.insert("basepath".to_owned(), "/home/planet".to_owned());
    let store = entities::Store {
        id: "cafebabe".to_owned(),
        store_type: entities::StoreType::LOCAL,
        label: "local disk".to_owned(),
        properties,
    };
    fallback.put_store(&store).unwrap();

    // test getting the store w/o a fallback, should return None
    let option = repo.get_store("cafebabe")?;
    assert!(option.is_none());

    // assign the fallback data source
    let fb_arc = Arc::new(fallback);
    repo.set_fallback(Some(fb_arc.clone()));

    // test getting the store w/fallback, should return store
    let option = repo.get_store("cafebabe")?;
    assert!(option.is_some());

    Ok(())
}
