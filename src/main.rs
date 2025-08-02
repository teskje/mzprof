mod aggregate;
mod collect;
mod pprof;
mod types;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use clap::Parser;
use futures::StreamExt;

use crate::aggregate::{ElapsedAggregator, SizeAggregator};
use crate::collect::{Collector, Event};
use crate::pprof::profile as pp;

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

    /// Output file path
    #[arg(long)]
    output_file: Option<PathBuf>,

    #[command(subcommand)]
    profile: Profile,
}

#[derive(Debug, Parser)]
enum Profile {
    /// Collect an elapsed time profile
    Time {
        /// Profiling duration in seconds
        #[arg(long)]
        duration: Option<u64>,
    },
    /// Collect a size profile
    Size,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let collector = Collector::new(&args.sql_url, &args.cluster, &args.replica)?;

    let prof = match args.profile {
        Profile::Time { duration } => {
            let duration = duration.map(Duration::from_secs);
            profile_time(collector, duration).await?
        }
        Profile::Size => profile_size(collector).await?,
    };

    let output_path = args.output_file.unwrap_or_else(|| match args.profile {
        Profile::Time { .. } => "time.pprof".into(),
        Profile::Size => "size.pprof".into(),
    });
    println!("Writing profile to file `{}`", output_path.display());
    pprof::write_file(&prof, &output_path)?;

    Ok(())
}

async fn profile_time(
    collector: Collector,
    duration: Option<Duration>,
) -> anyhow::Result<pp::Profile> {
    collector.subscribe_elapsed().await?;

    let mut event_stream = if let Some(duration) = duration {
        println!("Collecting time profile over {duration:?}");
        collector.listen(duration)
    } else {
        println!("Collecting time profile snapshot");
        collector.snapshot()
    };

    let mut aggregator = ElapsedAggregator::new();

    while let Some(event) = event_stream.next().await {
        match event {
            Event::Elapsed(batch) => {
                println!("* processing updates up to time {:?}", batch.time);
                aggregator.update(batch);
            }
            Event::Error(error) => bail!("collector error: {error}"),
            Event::Size(_) => unreachable!(),
        }
    }

    Ok(aggregator.build_pprof())
}

async fn profile_size(collector: Collector) -> anyhow::Result<pp::Profile> {
    collector.subscribe_size().await?;

    println!("Collecting size profile snapshot");
    let mut event_stream = collector.snapshot();

    let mut aggregator = SizeAggregator::new();

    while let Some(event) = event_stream.next().await {
        match event {
            Event::Size(batch) => {
                println!("* processing updates up to time {:?}", batch.time);
                aggregator.update(batch);
            }
            Event::Error(error) => bail!("collector error: {error}"),
            Event::Elapsed(_) => unreachable!(),
        }
    }

    Ok(aggregator.build_pprof())
}
