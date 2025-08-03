mod aggregate;
mod collect;
mod pprof;
mod types;

use std::time::Duration;

use clap::{Parser, ValueEnum};
use futures::TryStreamExt;

use crate::aggregate::Aggregator;
use crate::collect::{Collector, subscribe};

/// Dataflow profiler for Materialize
#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// URL of the Materialize SQL endpoint
    #[arg(long)]
    sql_url: String,

    /// Target cluster name
    #[arg(long)]
    cluster: String,

    /// Target replica name
    #[arg(long)]
    replica: String,

    /// Types of profiles to collect
    #[arg(
        long = "profile",
        value_enum,
        num_args(1..),
        value_delimiter(','),
        required = true,
    )]
    profiles: Vec<Profile>,

    /// Profiling duration in seconds
    #[arg(long)]
    duration: Option<u64>,

    /// Output file path
    #[arg(long, default_value_t = String::from("profile.pprof"))]
    output_file: String,
}

#[derive(Clone, Debug, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
enum Profile {
    /// elapsed time profile
    Time,
    /// heap size profile
    Size,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = Args::parse();

    args.profiles.sort();
    args.profiles.dedup();

    let mode = match args.duration {
        Some(secs) => {
            let duration = Some(Duration::from_secs(secs));
            subscribe::Mode::Continual { duration }
        }
        None => subscribe::Mode::Snapshot,
    };

    let mut collector = Collector::new(&args.sql_url, &args.cluster, &args.replica)?;
    collector.subscribe(subscribe::Operator, mode).await?;

    for profile in args.profiles {
        match profile {
            Profile::Time => collector.subscribe(subscribe::Elapsed, mode).await?,
            Profile::Size => collector.subscribe(subscribe::Size, mode).await?,
        }
    }

    let mut stream = collector.into_stream();
    let mut aggregator = Aggregator::new();

    while let Some(batch) = stream.try_next().await? {
        println!("* processing updates up to time {:?}", batch.time);
        aggregator.update(batch);
    }

    let prof = aggregator.build_pprof();

    println!("Writing profile to file `{}`", args.output_file);
    pprof::write_file(&prof, &args.output_file)?;

    Ok(())
}
