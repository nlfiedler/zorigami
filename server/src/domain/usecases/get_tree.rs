//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Tree};
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use std::cmp;
use std::fmt;

pub struct GetTree {
    repo: Box<dyn RecordRepository>,
}

impl GetTree {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Option<Tree>, Params> for GetTree {
    fn call(&self, params: Params) -> Result<Option<Tree>, Error> {
        self.repo.get_tree(&params.digest)
    }
}

pub struct Params {
    /// Hash digest of the tree to retrieve.
    digest: Checksum,
}

impl Params {
    pub fn new(digest: Checksum) -> Self {
        Self { digest }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.digest)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{TreeEntry, TreeReference};
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;
    use std::path::Path;

    #[test]
    fn test_get_tree_some() {
        // arrange
        let b3sum = "deb7853b5150885d2f6bda99b252b97104324fe3ecbf737f89d6cd8c781d1128";
        let file_digest = Checksum::BLAKE3(String::from(b3sum));
        let reference = TreeReference::FILE(file_digest);
        let filepath = Path::new("../test/fixtures/lorem-ipsum.txt");
        let entry = TreeEntry::new(filepath, reference);
        let tree = Tree::new(vec![entry], 1);
        let tree_sha1 = tree.digest.clone();
        let tree_sha2 = tree.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_sha1)
            .returning(move |_| Ok(Some(tree.clone())));
        // act
        let usecase = GetTree::new(Box::new(mock));
        let params = Params::new(tree_sha2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(actual.entries.len(), 1);
    }

    #[test]
    fn test_get_tree_none() {
        // arrange
        let tree_sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let tree_sha2 = tree_sha1.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_sha1)
            .returning(move |_| Ok(None));
        // act
        let usecase = GetTree::new(Box::new(mock));
        let params = Params::new(tree_sha2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn test_get_tree_err() {
        // arrange
        let tree_sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let tree_sha2 = tree_sha1.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_tree()
            .withf(move |d| d == &tree_sha1)
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = GetTree::new(Box::new(mock));
        let params = Params::new(tree_sha2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
