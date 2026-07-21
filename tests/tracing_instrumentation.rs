// Everything below runs inside one `#[test]` fn, deliberately -- this
// installs a `tracing::Subscriber` via `set_global_default`, which (unlike
// `with_default`) is genuinely process-wide, covering every OS thread
// including the blocking pool's, but can only be called once per process.
// A second `#[test]` fn calling it again would panic, so every scenario
// this file cares about is exercised sequentially against one shared
// recorder instead.
use rusty_tokio::task::{self, Builder, LocalSet};
use rusty_tokio::Runtime;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

#[derive(Default)]
struct RecordedSpan {
    target: String,
    name: String,
    fields: HashMap<String, String>,
    enters: u32,
    exits: u32,
}

#[derive(Default)]
struct Recorder {
    next_id: AtomicU64,
    spans: Mutex<HashMap<u64, RecordedSpan>>,
}

impl Recorder {
    fn count_matching(&self, target: &str, kind: &str) -> usize {
        self.spans
            .lock()
            .unwrap()
            .values()
            .filter(|s| {
                s.target == target && s.fields.get("kind").map(String::as_str) == Some(kind)
            })
            .count()
    }

    fn only_matching(&self, target: &str, kind: &str) -> RecordedSpan {
        let spans = self.spans.lock().unwrap();
        let mut matching: Vec<&RecordedSpan> = spans
            .values()
            .filter(|s| {
                s.target == target && s.fields.get("kind").map(String::as_str) == Some(kind)
            })
            .collect();
        assert_eq!(matching.len(), 1, "expected exactly one matching span");
        let m = matching.pop().unwrap();
        RecordedSpan {
            target: m.target.clone(),
            name: m.name.clone(),
            fields: m.fields.clone(),
            enters: m.enters,
            exits: m.exits,
        }
    }
}

struct FieldRecorder<'a>(&'a mut HashMap<String, String>);

impl Visit for FieldRecorder<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl Subscriber for Recorder {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut fields = HashMap::new();
        attrs.record(&mut FieldRecorder(&mut fields));
        let metadata = attrs.metadata();
        self.spans.lock().unwrap().insert(
            id,
            RecordedSpan {
                target: metadata.target().to_string(),
                name: metadata.name().to_string(),
                fields,
                enters: 0,
                exits: 0,
            },
        );
        Id::from_u64(id)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        if let Some(recorded) = self.spans.lock().unwrap().get_mut(&span.into_u64()) {
            values.record(&mut FieldRecorder(&mut recorded.fields));
        }
    }

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        if let Some(recorded) = self.spans.lock().unwrap().get_mut(&span.into_u64()) {
            recorded.enters += 1;
        }
    }

    fn exit(&self, span: &Id) {
        if let Some(recorded) = self.spans.lock().unwrap().get_mut(&span.into_u64()) {
            recorded.exits += 1;
        }
    }
}

#[test]
fn console_subscriber_shaped_instrumentation() {
    let recorder = Arc::new(Recorder::default());
    tracing::subscriber::set_global_default(recorder.clone())
        .expect("no other subscriber has been installed in this process yet");

    let rt = Runtime::new().unwrap();

    // A plain `rusty_tokio::spawn` gets a span shaped exactly the way
    // real tokio's own `runtime.spawn` span is shaped: target
    // "tokio::task", name "runtime.spawn", `kind` == "task".
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            task::yield_now().await;
            task::yield_now().await;
        });
        handle.await.unwrap();
    });
    let plain = recorder.only_matching("tokio::task", "task");
    assert_eq!(plain.name, "runtime.spawn");
    assert_eq!(plain.fields.get("task.name").map(String::as_str), Some(""));
    assert!(plain
        .fields
        .get("task.id")
        .and_then(|s| s.parse::<u64>().ok())
        .is_some());
    assert!(plain.fields.contains_key("loc.file"));
    assert!(plain.fields.contains_key("loc.line"));
    assert!(plain.fields.contains_key("loc.col"));
    // Entered at least twice (two intervening `yield_now`s force at
    // least three polls total) and exited the same number of times --
    // no dangling "currently entered" state once the task has finished.
    assert!(plain.enters >= 2);
    assert_eq!(plain.enters, plain.exits);

    // `task::Builder::spawn`'s name reaches the span's `task.name` field.
    rt.block_on(async {
        let handle = Builder::new().name("my-named-task").spawn(async {});
        handle.await.unwrap();
    });
    let named_count_before_check = recorder
        .spans
        .lock()
        .unwrap()
        .values()
        .filter(|s| s.fields.get("task.name").map(String::as_str) == Some("my-named-task"))
        .count();
    assert_eq!(named_count_before_check, 1);

    // `task::spawn_local` (inside a `LocalSet`) gets the same kind of
    // span as an ordinary spawned task.
    let before_local = recorder.count_matching("tokio::task", "task");
    let local = LocalSet::new();
    local.run_until(async {
        let handle = task::spawn_local(async { 1 + 1 });
        assert_eq!(handle.await.unwrap(), 2);
    });
    assert_eq!(
        recorder.count_matching("tokio::task", "task"),
        before_local + 1
    );

    // `spawn_blocking` produces *two* independent spans: the blocking
    // closure's own (target "tokio::task::blocking", kind "blocking"),
    // entered for its actual execution on the blocking-pool thread, and
    // the ordinary rendezvous wrapper task's (target "tokio::task", kind
    // "task") -- see `task::trace`'s module docs for why.
    let before_blocking = recorder.count_matching("tokio::task::blocking", "blocking");
    let before_wrapper = recorder.count_matching("tokio::task", "task");
    rt.block_on(async {
        let handle = rusty_tokio::spawn_blocking(|| 1 + 1);
        assert_eq!(handle.await.unwrap(), 2);
    });
    assert_eq!(
        recorder.count_matching("tokio::task::blocking", "blocking"),
        before_blocking + 1
    );
    assert_eq!(
        recorder.count_matching("tokio::task", "task"),
        before_wrapper + 1
    );
    let blocking = recorder.only_matching("tokio::task::blocking", "blocking");
    assert_eq!(blocking.name, "runtime.spawn");
    assert!(blocking.enters >= 1);
    assert_eq!(blocking.enters, blocking.exits);
}
