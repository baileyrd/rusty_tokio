//! [`Builder`]: an alternative to the plain [`crate::spawn`]/
//! [`crate::runtime::Handle::spawn`] free functions that lets a task
//! carry a human-readable name -- retrievable from inside the running
//! task itself via [`super::try_name`].

use super::JoinHandle;
use crate::runtime::Handle;
use std::future::Future;
use std::sync::Arc;

/// Configures a task before spawning it -- currently just an optional
/// name.
///
/// ```
/// # use rusty_tokio::Runtime;
/// # let rt = Runtime::new().unwrap();
/// # rt.block_on(async {
/// let handle = rusty_tokio::task::Builder::new()
///     .name("my-task")
///     .spawn(async { rusty_tokio::task::try_name() });
/// let name = handle.await.unwrap();
/// assert_eq!(name.as_deref(), Some("my-task"));
/// # });
/// ```
pub struct Builder<'a> {
    name: Option<&'a str>,
}

impl<'a> Builder<'a> {
    pub fn new() -> Self {
        Builder { name: None }
    }

    pub fn name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    /// Spawns `future` onto the currently running runtime, same as
    /// [`crate::spawn`], but carrying this builder's name.
    ///
    /// Unlike tokio's own `task::Builder::spawn`, this returns the
    /// `JoinHandle` directly rather than `io::Result<JoinHandle<T>>` --
    /// nothing about spawning here can actually fail (no per-task
    /// allocation this crate does for a name or task-local storage is
    /// fallible), so wrapping it in a `Result` callers would always just
    /// `.unwrap()` didn't seem worth the ceremony.
    ///
    /// # Panics
    /// Panics if called from a thread with no ambient runtime -- same as
    /// [`crate::spawn`].
    pub fn spawn<F>(self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = self.name.map(Arc::from);
        let handle = Handle::current();
        super::spawn_named(&handle.shared, future, name)
    }
}

impl Default for Builder<'_> {
    fn default() -> Self {
        Self::new()
    }
}
