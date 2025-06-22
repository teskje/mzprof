mod aggregate;
mod collect;
mod pprof;
mod types;

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use clap::Parser;
use futures::StreamExt;

use crate::aggregate::Aggregator;
use crate::collect::{Collector, Event};

/// Dataflow profiler for Materialize
#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// URL of the Materialize SQL endpoint
    sql_url: String,

    /// Target cluster name
    #[arg(long)]
    cluster: String,

    /// Target replica name
    #[arg(long)]
    replica: String,

    /// Profiling duration in seconds
    #[arg(long)]
    duration: Option<u64>,

    /// Output file path
    #[arg(long)]
    output_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let collector = Collector::new(&args.sql_url, &args.cluster, &args.replica)?;
    collector.subscribe_elapsed().await?;

    let mut event_stream = if let Some(secs) = args.duration {
        let duration = Duration::from_secs(secs);
        println!("Collecting profile for {duration:?}");
        collector.listen(duration)
    } else {
        println!("Collecting snapshot profile");
        collector.snapshot()
    };

    let mut aggregator = Aggregator::new();

    while let Some(event) = event_stream.next().await {
        match event {
            Event::Elapsed(batch) => {
                println!("* processing updates up to time {:?}", batch.time);
                aggregator.update_elapsed(batch);
            }
            Event::Error(error) => bail!("collector error: {error}"),
        }
    }

    let output_path = args.output_file.unwrap_or("time.pprof".into());
    println!("Writing profile to file `{}`", output_path.display());

    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);
    aggregator.write_pprof(&mut writer)?;
    writer.flush()?;

    Ok(())
}
