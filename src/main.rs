mod aggregate;
mod collect;
mod pprof;
mod types;

use std::time::Duration;

use clap::Parser;
use futures::TryStreamExt;

use crate::aggregate::Aggregator;
use crate::collect::Collector;

/// Dataflow profiler for Materialize
#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    #[command(flatten)]
    profile: Profile,

    /// URL of the Materialize SQL endpoint
    #[arg(long)]
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
    #[arg(long, default_value_t = String::from("profile.pprof"))]
    output_file: String,
}

#[derive(Debug, Parser)]
#[group(required = true)]
struct Profile {
    /// Collect an elapsed time profile
    #[arg(long)]
    time: bool,

    /// Collect a heap size profile
    #[arg(long)]
    size: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut collector = Collector::new(&args.sql_url, &args.cluster, &args.replica)?;
    collector.subscribe_operator().await?;

    if args.profile.time {
        collector.subscribe_elapsed().await?;
    }
    if args.profile.size {
        collector.subscribe_size().await?;
    }

    let mut stream = if let Some(secs) = args.duration {
        let duration = Duration::from_secs(secs);
        println!("Collecting profile over {duration:?}");
        collector.listen(duration)
    } else {
        println!("Collecting profile snapshot");
        collector.snapshot()
    };

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
