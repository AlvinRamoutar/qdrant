//! Hierarchical progress tracker.
//!
//! Tracks progress of nested tasks.
//!
//! A _span_ is a task with a name, start time, and optional end time.
//! Similar to tracing/OpenTelemetry spans.
//! It can be either:
//! - a _branch span_ that can have child spans (aka subtasks).
//! - a _leaf span_ that tracks progress as a number of items done.
//!
//! # Example
//!
//! ```text
//! ─────────────────────────── Time ────────────────────────────▶
//!
//! ┌─────────────────────────────── Segment Indexing ───────────
//! │┌─────── Quantization ───────┐┌───── HNSW Index Building ───
//! ││┌─ Vector A ─┐┌─ Vector B ─┐││┌─ Vector A ─┐┌─ Vector B ───
//! │││ ########## ││ ########## ││││ ########## ││ #########....
//! ││└────────────┘└────────────┘││└────────────┘└──────────────
//! │└────────────────────────────┘└─────────────────────────────
//! └────────────────────────────────────────────────────────────
//! ```
//!
//! "Segment Indexing", "Quantization", and "HNSW Index Building" are branch
//! spans.
//! "Vector A" and "Vector B" are leaf spans that track progress.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Serialize;

/// A read-only view of a root progress span.
///
/// Keep it around to observe the progress from another thread.
#[derive(Clone)]
pub struct ProgressView(Arc<Mutex<SpanInner>>);

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ProgressEntry {
    pub stage: Vec<String>,
    pub started: DateTime<Utc>,
    pub stage_started: DateTime<Utc>,
    pub stage_progress: Option<(u64, u64)>, // (done, total)
}

/// A branch span that can have child spans.
///
/// Can be used to create child spans/subtasks.
pub struct ProgressSpanBranch {
    root: Arc<Mutex<SpanInner>>,
    position: Vec<usize>,
}

/// A leaf span that tracks progress as a number of items done.
///
/// Can be used to signal progress updates.
pub struct ProgressSpanLeaf {
    root: Arc<Mutex<SpanInner>>,
    position: Vec<usize>,
    progress: Arc<AtomicU64>,
}

struct SpanInner {
    kind: SpanKind,
    started: DateTime<Utc>,
    finished: Option<DateTime<Utc>>,
}

enum SpanKind {
    Branch {
        children: Vec<(String, SpanInner)>,
    },
    Leaf {
        progress: Arc<AtomicU64>,
        total: u64,
    },
}

/// Create a new progress tracker.
///
/// Returns a [`ProgressView`] to observe the progress, and a
/// [`ProgressSpanBranch`] to signal progress updates.
pub fn new_progress_tracker() -> (ProgressView, ProgressSpanBranch) {
    let root = Arc::new(Mutex::new(SpanInner {
        kind: SpanKind::Branch {
            children: Vec::new(),
        },
        started: Utc::now(),
        finished: None,
    }));
    (
        ProgressView(root.clone()),
        ProgressSpanBranch {
            root,
            position: Vec::new(),
        },
    )
}

impl ProgressView {
    pub fn snapshot(&self) -> Option<ProgressEntry> {
        let root = self.0.lock();
        Self::find_active(&root, &mut Vec::new(), root.started)
    }

    fn find_active(
        span: &SpanInner,
        path: &mut Vec<String>,
        root_started: DateTime<Utc>,
    ) -> Option<ProgressEntry> {
        if span.finished.is_some() {
            return None;
        }
        match &span.kind {
            SpanKind::Branch { children } => {
                for (name, child) in children {
                    path.push(name.clone());
                    if let Some(entry) = Self::find_active(child, path, root_started) {
                        return Some(entry);
                    }
                    path.pop();
                }
                Some(ProgressEntry {
                    stage: path.clone(),
                    started: root_started,
                    stage_started: span.started,
                    stage_progress: None,
                })
            }
            SpanKind::Leaf { progress, total } => Some(ProgressEntry {
                stage: path.clone(),
                started: root_started,
                stage_started: span.started,
                stage_progress: Some((progress.load(Ordering::Relaxed), *total)),
            }),
        }
    }
}

impl ProgressSpanBranch {
    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_test() -> Self {
        new_progress_tracker().1
    }

    /// Create a branch child span - a span that can have its own child spans.
    pub fn make_branch(&self, name: &str) -> ProgressSpanBranch {
        let position = self.append_child(
            name,
            SpanKind::Branch {
                children: Vec::new(),
            },
        );
        ProgressSpanBranch {
            root: self.root.clone(),
            position,
        }
    }

    /// Create a leaf child span - a span that tracks the progress.
    pub fn make_leaf(&self, name: &str, total: u64) -> ProgressSpanLeaf {
        let progress = Arc::new(AtomicU64::new(0));
        let position = self.append_child(
            name,
            SpanKind::Leaf {
                progress: progress.clone(),
                total,
            },
        );
        ProgressSpanLeaf {
            root: self.root.clone(),
            position,
            progress,
        }
    }

    /// Append a child span to this branch span.
    /// Returns the position of the new child.
    fn append_child(&self, name: &str, kind: SpanKind) -> Vec<usize> {
        let mut root = self.root.lock();
        if let Some(parent) = root.get_mut(&self.position)
            && let SpanKind::Branch { children } = &mut parent.kind
        {
            let mut position = Vec::with_capacity(self.position.len() + 1);
            position.extend_from_slice(&self.position);
            position.push(children.len());

            children.push((
                name.to_string(),
                SpanInner {
                    kind,
                    started: Utc::now(),
                    finished: None,
                },
            ));

            position
        } else {
            // Should never happen. But if it does, return an obviously invalid
            // position to avoid a panic.
            debug_assert!(false, "Invalid span position for appending child");
            vec![usize::MAX, usize::MAX]
        }
    }
}

impl Drop for ProgressSpanBranch {
    fn drop(&mut self) {
        SpanInner::finish(&self.root, &self.position);
    }
}

impl ProgressSpanLeaf {
    pub fn progress(&self) -> &Arc<AtomicU64> {
        &self.progress
    }
}

impl Drop for ProgressSpanLeaf {
    fn drop(&mut self) {
        SpanInner::finish(&self.root, &self.position);
    }
}

impl SpanInner {
    fn get_mut(&mut self, position: &[usize]) -> Option<&mut SpanInner> {
        let mut current = &mut *self;
        for &idx in position {
            match &mut current.kind {
                SpanKind::Branch { children } => current = &mut children.get_mut(idx)?.1,
                SpanKind::Leaf { .. } => return None,
            }
        }
        Some(current)
    }

    fn finish(root: &Arc<Mutex<SpanInner>>, position: &[usize]) {
        let mut root = root.lock();
        if let Some(span) = root.get_mut(position) {
            span.finished = Some(Utc::now());
        } else {
            // Should never happen.
            debug_assert!(false, "Cannot finish a non-existent span");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_tracker() {
        let (view, span) = new_progress_tracker();

        {
            let span_foo = span.make_branch("foo");
            {
                let span_foo_bar = span_foo.make_branch("bar");
                {
                    let span_foo_bar_leaf = span_foo_bar.make_leaf("leaf", 10);
                    span_foo_bar_leaf.progress().fetch_add(3, Ordering::Relaxed);
                    check_snapshot(&view, vec!["foo", "bar", "leaf"], Some((3, 10)));
                }
                check_snapshot(&view, vec!["foo", "bar"], None);
                {
                    let span_foo_bar_leaf2 = span_foo_bar.make_leaf("leaf2", 5);
                    span_foo_bar_leaf2
                        .progress()
                        .fetch_add(5, Ordering::Relaxed);
                    check_snapshot(&view, vec!["foo", "bar", "leaf2"], Some((5, 5)));
                }
            }
            check_snapshot(&view, vec!["foo"], None);
        }
        check_snapshot(&view, vec![], None);

        drop(span);
        assert!(view.snapshot().is_none());
    }

    fn check_snapshot(
        view: &ProgressView,
        expected_stage: Vec<&str>,
        expected_progress: Option<(u64, u64)>,
    ) {
        let ProgressEntry {
            stage,
            started: _,       // ignore timestamps
            stage_started: _, // ditto
            stage_progress,
        } = view.snapshot().expect("Expected active span");

        let stage: Vec<&str> = stage.iter().map(String::as_str).collect();

        assert_eq!(stage, expected_stage);
        assert_eq!(stage_progress, expected_progress);
    }
}
