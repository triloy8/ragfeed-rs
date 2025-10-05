use std::sync::{Arc, Mutex, OnceLock, atomic::{AtomicBool, AtomicUsize, Ordering}};

use anyhow::Result;

use crate::output::config::OutputConfig;
use crate::output::Emitter;
use crate::output::types::Envelope;

/// Placeholder for future structured telemetry events.
#[derive(Debug)]
pub struct EventPayload<'a> {
    pub kind: &'a str,
}

pub trait OutputSink: Send + Sync {
    fn on_plan(&self, env: &Envelope) -> Result<()>;
    fn on_result(&self, env: &Envelope) -> Result<()>;

    fn on_event(&self, _event: &EventPayload<'_>) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct StdoutSink;

impl OutputSink for StdoutSink {
    fn on_plan(&self, env: &Envelope) -> Result<()> {
        if stdout_disabled() {
            return Ok(());
        }
        emit_to_stdout(env)
    }

    fn on_result(&self, env: &Envelope) -> Result<()> {
        if stdout_disabled() {
            return Ok(());
        }
        emit_to_stdout(env)
    }
}

fn emit_to_stdout(env: &Envelope) -> Result<()> {
    let cfg = OutputConfig::from_env();
    let emitter = Emitter::from_env(cfg);
    emitter.emit(env).map_err(anyhow::Error::from)
}

type DynSink = Arc<dyn OutputSink>;

fn sink_slot() -> &'static Mutex<DynSink> {
    static SINK: OnceLock<Mutex<DynSink>> = OnceLock::new();
    SINK.get_or_init(|| Mutex::new(Arc::new(StdoutSink::default()) as DynSink))
}

pub fn current_sink() -> DynSink {
    sink_slot().lock().expect("sink mutex poisoned").clone()
}

pub struct SinkGuard {
    previous: DynSink,
}

pub fn install_sink(new_sink: DynSink) -> SinkGuard {
    let slot = sink_slot();
    let mut guard = slot.lock().expect("sink mutex poisoned");
    let previous = guard.clone();
    *guard = new_sink;
    let prev = stdout_disable_counter().fetch_add(1, Ordering::SeqCst);
    if prev == 0 {
        stdout_disabled_flag().store(true, Ordering::SeqCst);
    }
    SinkGuard { previous }
}

impl Drop for SinkGuard {
    fn drop(&mut self) {
        let slot = sink_slot();
        let mut guard = slot.lock().expect("sink mutex poisoned");
        *guard = self.previous.clone();
        if stdout_disable_counter().fetch_sub(1, Ordering::SeqCst) == 1 {
            stdout_disabled_flag().store(false, Ordering::SeqCst);
        }
    }
}

fn stdout_disable_counter() -> &'static AtomicUsize {
    static COUNTER: OnceLock<AtomicUsize> = OnceLock::new();
    COUNTER.get_or_init(|| AtomicUsize::new(0))
}

fn stdout_disabled_flag() -> &'static AtomicBool {
    static FLAG: OnceLock<AtomicBool> = OnceLock::new();
    FLAG.get_or_init(|| AtomicBool::new(false))
}

fn stdout_disabled() -> bool {
    stdout_disabled_flag().load(Ordering::SeqCst)
}
