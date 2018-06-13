# Overview

This project demonstrates an apparent bug which Penske is experiencing.  They
are using GemFire as part of their streaming ingest process.  Truck telemetry
entries are inserted into a region via SpringXD using the "gemfire-json-server"
sink, which sits on top of SDG 1.7 and GemFire 8.0.0.   The server side is
running GemFire 8.2.8.

The recent change for them is partitioning their truck telemetry region by
vehicle VIN so there is reason to suspect that a custom partition resolver
is a necessary ingredient of this problem.  Their partition resolver looks for
the first occurrence of '|' in the key and takes everything to the left as
the routing object.  This is built-in functionality in later versions of GemFire
but not in theirs.

They have a purge job, which is a client program  
1) issues a query for all keys associated with a certain day (regardless of vin)
2) calls remove all on those keys

They noticed that there were some keys that would not be deleted. They could
see the key using a query but could not remove it with a "removeAll".  

I have been able to reproduce something very similar and I will add that
the entry in question cannot even be deleted with a "remove" call from gfsh
although a query shows it is there.

# Walk Through

Download submodules

```
git submodule init
git submodule update
```

Review the partition resolver in the `partition-by-zip` maven project , then
__install__ it.

```
cd partition-by-zip
mvn install
cd ..
```

Edit `env.sh` to point to a GemFire 8.2.8 installation and a compatible JDK.
When you are done _source_ it (don't run it).

```
. env.sh
```

In addition to this file, the GemFire cluster in this example is configured
via 2 files:

- `cache.xml`
- `cluster.json`

Review these files to understand the configuration in use.  It is a 3 node
cluster with 2 PARTITION_REDUNDANT_PERSISTENT_OVERFLOW regions.  The
`person2` region which is used by this example also has the above partition
resolver configured.

A quick overview of cluster operations follows.

start: `python cluster.py start`
stop: `python cluster.py stop`
remove all state and start over: `rm -rf datanode{1,2,3} locator1`

To learn more, see: https://github.com/Pivotal-Data-Engineering/gemfire-manager


Now start the cluster:

```
python cluster.py start
```

Cluster connection information:
locator: `localhost[10000]`
pulse: `http://localhost:10070/pulse`

Now review the loader program in `people-loader`.  

- review the pom file and notice the software version of SDG and GemFire
- all configuration is in src/main/resources/application.xml
- note that SDG is being used and review the client config (this mimics
  the client config in Spring XD gemfire-json-server as far as I can tell).
- The program generates and puts fake "people" entries using a pipe delimited
  string key corresponding to the person's zip code (this is analogous to
  Penske's VIN).
- The program is multi-threaded.
- When run it generates and puts 10,000 people entries to the `person2` region
  using 4 threads.
- It has the partition resolver on its class path (see the partition-by-zip
  dependency).

Run it with no parameters (I ran from within the IDE)

Review the `ClearByZip` Function, then deploy it and run it:

```
$GEMFIRE/bin/gfsh
gfsh>connect --locator=localhost[10000]
gfsh>deploy --jar=people-loader/target/people-loader-1.0-SNAPSHOT.jar
gfsh>execute function --id=ClearByZip --region=person2 --arguments='00001'
```
