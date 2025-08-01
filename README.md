# mzprof

A dataflow profiler for [Materialize](https://github.com/MaterializeInc/materialize).

`mzprof` is a CLI tool that connects to any Materialize environment and subscribes to [introspection relations](https://materialize.com/docs/sql/system-catalog/mz_introspection) to collect metrics about running dataflows.
It produces profiles in [pprof](https://github.com/google/pprof) format, which can be analyzed using any tool supporting that format.

Currently `mzprof` only supports collecting one-off elapsed time profiles.
Support for memory profiles and continuous profiling is planned.

## Usage

To collect a profile you need to supply `mzprof` with the `postgres://` URL of the target Materialize environment, as well as a cluster and replica name.

```
cargo run -- postgres://jan@localhost:6875/materialize --cluster compute --replica r1
```

This will collect a time profile for all dataflows on the target replica, including their elapsed time since they were installed.
You can instead collect a profile of live elapsed times by specifying a listen duration in seconds with the `--duration` flag.

To conveniently view the profile, you can upload it to https://pprof.me.
Its UI provides a bunch of knobs to customize the displayed flame graph:

* To show time spent per worker, select the "worker" label in the `Group by` dropdown.
* To zoom in on a single dataflow, use the `Filter` menu to add a stack filter for the dataflow name.
* To group by operator ID instead of operator name, select "Address" under `Preferences > Levels`.
