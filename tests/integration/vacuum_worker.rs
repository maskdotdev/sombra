#![allow(missing_docs)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::{
    DeleteNodeOpts, Graph, GraphOptions, NodeSpec, PropEntry, PropValue, VacuumCfg, VacuumTrigger,
};
use sombra::types::{LabelId, PropId, Result};
use tempfile::{tempdir, TempDir};

fn small_vacuum_cfg(interval: Duration, high_water_bytes: u64) -> VacuumCfg {
    VacuumCfg {
        enabled: true,
        interval,
        retention_window: Duration::from_millis(0),
        log_high_water_bytes: high_water_bytes,
        max_pages_per_pass: 64,
        max_millis_per_pass: 100,
        index_cleanup: true,
    }
}

fn setup_graph(cfg: VacuumCfg) -> Result<(TempDir, Arc<Pager>, Arc<Graph>)> {
    let dir = tempdir()?;
    let path = dir.path().join("vacuum_worker.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store).vacuum(cfg))?;
    Ok((dir, pager, graph))
}

#[test]
fn background_vacuum_runs_with_timer() -> Result<()> {
    let (_dir, pager, graph) = setup_graph(small_vacuum_cfg(Duration::from_millis(25), 0))?;

    let mut write = pager.begin_write()?;
    let node = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
        },
    )?;
    pager.commit(write)?;

    let mut write = pager.begin_write()?;
    graph.delete_node(&mut write, node, DeleteNodeOpts::default())?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;
    let write = pager.begin_write()?;
    pager.commit(write)?;

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(stats) = graph.last_vacuum_stats() {
            if stats.trigger == VacuumTrigger::Timer {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            panic!("vacuum worker did not run in time");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn high_water_trigger_fires() -> Result<()> {
    let (_dir, pager, graph) = setup_graph(small_vacuum_cfg(Duration::from_secs(60), 1))?;

    let mut write = pager.begin_write()?;
    let node = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("hello"))],
        },
    )?;
    pager.commit(write)?;

    let mut write = pager.begin_write()?;
    graph.delete_node(&mut write, node, DeleteNodeOpts::default())?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(stats) = graph.last_vacuum_stats() {
            if stats.trigger == VacuumTrigger::HighWater {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            panic!("high-water trigger not observed");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
