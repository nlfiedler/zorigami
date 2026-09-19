//
// Copyright (c) 2022 Nathan Fiedler
//
pub mod backup;
pub mod leader;
pub mod prune;
pub mod restore;
pub mod schedule;

/// A single problem discovered while a background task was running.
#[derive(Clone, Debug)]
pub struct ScrubIssue {
    /// Identifier of the dataset this issue relates to, if any.
    pub dataset_id: Option<String>,
    /// Human-readable description of the problem.
    pub message: String,
}

/// What a background task did, reported back to the supervisor so it can be
/// recorded as a `TaskRun` and shown to the user.
///
/// The summary is composed by the task itself, since only the task knows what
/// is worth counting. The supervisor turns this into an outcome: skipped if
/// the task had nothing to do, otherwise successful or not according to
/// whether any issues were reported.
#[derive(Clone, Debug, Default)]
pub struct TaskReport {
    /// Problems encountered during the run. Each is also captured as an error
    /// so it shows up in the error log.
    pub issues: Vec<ScrubIssue>,
    /// Human-readable description of what the run did.
    pub summary: String,
    /// True when the task returned without performing its work, which is not
    /// the same as performing it successfully.
    pub skipped: bool,
}

impl TaskReport {
    /// A run that did its work, described by the given summary.
    pub fn done<S: Into<String>>(summary: S) -> Self {
        Self {
            issues: Vec::new(),
            summary: summary.into(),
            skipped: false,
        }
    }

    /// A run that had nothing to do, for the given reason.
    pub fn skipped<S: Into<String>>(reason: S) -> Self {
        Self {
            issues: Vec::new(),
            summary: reason.into(),
            skipped: true,
        }
    }

    /// Attach the issues encountered during the run.
    pub fn with_issues(mut self, issues: Vec<ScrubIssue>) -> Self {
        self.issues = issues;
        self
    }
}
