# mzprof

![crates.io](https://img.shields.io/crates/v/mzprof)
![build](https://img.shields.io/github/actions/workflow/status/teskje/mzprof/ci.yml)

A dataflow profiler for [Materialize](https://github.com/MaterializeInc/materialize).

`mzprof` is a CLI tool that connects to any Materialize environment and subscribes to [introspection relations](https://materialize.com/docs/sql/system-catalog/mz_introspection) to collect operator-level metrics about running dataflows.
It produces profiles in [pprof](https://github.com/google/pprof) format, which can be analyzed using any tool supporting that format.

`mzprof` supports collecting one-off elapsed time and memory size profiles.
Support for continuous profiling is planned.

## Installation

To install the latest published version of `mzprof` run:

```
cargo install --locked mzprof
```

Alternatively, clone this repository and use `cargo run` and/or `cargo build` as usual.

## Usage

To collect a profile you need to supply `mzprof` with the `postgres://` URL of the target Materialize environment, as well as a cluster and replica name:

```
mzprof postgres://jan@localhost:6875/materialize --cluster compute --replica r1 time
```

This will collect a time profile over all dataflows on the target replica, with their elapsed times since they were installed.

You can instead collect a profile of live elapsed times by specifying a listen duration in seconds:

```
mzprof [...] time --duration 10
```

To collect a size profile, use the `size` command instead:

```
mzprof [...] size
```

## Viewing Profiles

A convenient way to view profiles created by `mzprof` is uploading them to https://pprof.me.
It renders them as flame graphs with a bunch of knobs to customize the presentation:

* To show time spent per worker, select the "worker" label in the `Group by` dropdown.
* To zoom in on a single dataflow, use the `Filter` menu to add a stack filter for the dataflow name.
* To group by operator ID instead of operator name, select "Address" under `Preferences > Levels`.
