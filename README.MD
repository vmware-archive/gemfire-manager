# Overview

This project is a local environment consisting of one locator and 2 data nodes
which can be used for local development and testing.

# Reqirements

Python 2 or 3
A JDK
A GemFire 9.x installation

# Setup

1. If it is not already present, install Python from
   https://www.python.org/downloads/release/python-365/ (Python 2 is OK too)

2. If not already present, install JDK version 1.8 (not just a JRE) from
   http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

3. Download GemFire from: http://download.pivotal.com.s3.amazonaws.com/gemfire/9.3.0/pivotal-gemfire-9.3.0.zip
   and unzip it into a directory _that does not have spaces in its name_.

4. Edit cluster.json to point to the locations where you have installed GemFire
   and java.  For example:
   ```
   {
       "global-properties":{
           "gemfire": "D:/Users/rmay/pivotal-gemfire-9.3.0",
           "java-home" : "C:/Program Files/Java/jdk1.8.0_171",
           ...
   }
   ```

5. Initialize the cluster. From the command line, cd to the directory containing
cluster.json and run:  `python initcluster.py`

That completes the setup.  You can now use your cluster.

# Cluster Administration

## Start and Stop

Start the cluster: `python cluster.py start`

Stop the cluster: `python cluster.py stop`

## Start Over

This will remove all regions and persistent data.

- stop the cluster (see above)
- remove the working directories for each process (e.g. "locator1", "datanode1" , "datanode2")
- run the initcluster.py script again

## Useful Gfsh Commands

gfsh is in the "bin" folder of the GemFire distribution.  Use "gfsh.bat" for
Windows and "gfsh" for other OSs.

- connect: `connect --locator=localhost[10000]`  
- list members (must be connected): `list members`
- list regions (must be connected): `list regions`
- create a new partitioned region with 2 copies and no persistence (must be connected)
`create region --name=dev3_myregion` --type=PARTITION_REDUNDANT

## Other useful information

- log files: datanode{1,2}/server.log, locator1/locator.log
- locator port: 10000
- Pulse Web UI: http://localhost:17070/pulse (log in with "admin"/"admin")

# How it works

All configuration resides in a single configuration file, _cluster.json_,
which can be versioned.

An example is shown below.

```json
{
    "global-properties":{
        "gemfire": "/runtime/gemfire",
        "java-home" : "/runtime/java",
        "locators" : "10.0.0.101[10000]",
        "cluster-home" : "/runtime/cluster1"
    },
   "locator-properties" : {
        "port" : 10000,
        "jmx-manager-port" : 11099,
        "http-service-port" : 17070,
        "jmx-manager" : "true",
        "jmx-manager-start" : "true",
        "log-level" : "config",
        "statistic-sampling-enabled" : "true",
        "statistic-archive-file" : "locator.gfs",
        "log-file-size-limit" : "10",
        "log-disk-space-limit" : "100",
        "archive-file-size-limit" : "10",
        "archive-disk-space-limit" : "100",
        "jvm-options" : ["-Xmx8g","-Xms8g", "-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC"]
    },
   "datanode-properties" : {
        "server-port" : 10100,
        "conserve-sockets" : false,
        "log-level" : "config",
        "statistic-sampling-enabled" : "true",
        "statistic-archive-file" : "datanode.gfs",
        "log-file-size-limit" : "10",
        "log-disk-space-limit" : "100",
        "archive-file-size-limit" : "10",
        "archive-disk-space-limit" : "100",
        "jvm-options" : ["-Xmx12g","-Xms12g","-Xmn2g", "-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC", "-XX:CMSInitiatingOccupancyFraction=75"]
    },
    "hosts": {
        "hostA" : {
            "host-properties" :  {
            },
            "processes" : {
                "locator" : {
                    "type" : "locator",
                    "bind-address": "10.0.0.101",
                    "http-service-bind-address" : "10.0.0.101",
                    "jmx-manager-bind-address" : "10.0.0.101"
                }
            },
            "ssh" : {
                "host" : "52.91.207.138",
                "user" : "root",
                "key-file" : "my-keypair.pem"
             }
        },
        "hostB" : {
            "host-properties" :  {
            },
            "processes" : {
                "server111" : {
                    "type" : "datanode",
                    "bind-address": "10.0.0.111",
                    "server-bind-address" : "10.0.0.111"
                 }
            },
            "ssh" : {
                "host" : "54.83.157.253",
                "user" : "root",
                "key-file" : "my-keypair.pem"
            }
        }
    }
}
```

__No other configuration is required__

Notes About cluster.json

- The settings in _cluster.json_ are the same settings that can be put in
_gemfire.properties_ or provided on the gfsh command line.
- the hierarchical structure minimizes repeated configuration.
- each host has an _ssh_ section that includes the information needed to ssh to that host

_Simply put, gemfire-manager uses the information in cluster.json to ssh  to one or more machines in the cluster and issue gfsh commands_.


# The Cluster Configuration File#

Two sample cluster configuration filea are shown below. One is for a local cluster
and the other is for a remote cluster.  Note that the file is hierarchical in
nature.  This is so that settings that are common to all members can be shared
and do not need to be repeated.

Members look up their setting starting with the most specific portion of the
hierarchy and proceeding to the most general. The lookup algorithm is detailed
below.


1.  __host and process specific__ (see for example the _server-port_ settings in
    the sample below)
2. if there are no host and process specific settings, check __host__ settings
    ( the sample has no setting at the host level but they would be found inside of
    _hosts["localhost"]["host_properties"]_ )
3. if there are no host specific settings, check the __global settings for the
    process type__. (In the example, _conserve_sockets_ is set at this level).
    There is one section that applies to all data nodes (_datanode-properties_),
    and one that applies to locators (_locator-properties_).
4. Lastly, look in _global-properties_

#### Additional notes about the cluster configuration file ####
* place holders of the form ${ENV_VAR} can be used to pull in values from the
environment
* the __hosts__ section is a dictionary of each host in the cluster.  The key of
each dictionary entry must match the host name of the host as reported by the
_hostname_ command. _localhost_ is a special key that matches every host and
is useful for setting up local clusters.
* the _processes_ section within each host is a dictionary of processes that should
run on that host.  The key to the dictionary entry is used as the gemfire process
name (i.e. it is passed to gfsh with the --name option). _Each process must have
a name that is unique in the whole cluster_, not just on the host.
* All of the settings you can place in  _gemfire.properties_ are understood by
the scripts and can be used in the cluster configuration file.  There is special
logic built in to the scripts to understand options that can only be passed as
arguments to gfsh.  For example: _server-bind-address_ and _classpath_. The scripts
figure out which settings need to be passed as _--J=-Dgemfire.setting_ and which
are passed directly to gfsh as _--setting_.
* Place JVM gc options, arbitrary -Ds and other settings unrelated to gemfire in
the _jvm-options_ setting, wich is a list.
* for remote clusters, each host has an "ssh" object which specifies all of
the information needed to connect to that host.

#### Local Cluster Configuration File ####
```json
{
    "global-properties":{
        "gemfire": "${GEMFIRE}",
        "java-home" : "${JAVA_HOME}",
        "locators" : "localhost[10000]",
        "cluster-home" : "/tmp/gemfire"
    },
   "locator-properties" : {
        "bind-address": "localhost",
        "port" : 10000,
        "jmx-manager-port" : 11099,
        "http-service-bind-address" : "localhost",
        "http-service-port" : 17070,
        "jvm-options" : ["-Xmx1g","-Xms1g", "-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC"]
    },
   "datanode-properties" : {
        "bind-address" : "localhost",
        "server-bind-address" : "localhost",
        "conserve-sockets" : false,
        "jvm-options" : ["-Xmx3g","-Xms3g","-Xmn1g", "-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC", "-XX:CMSInitiatingOccupancyFraction=75"]
    },
    "hosts": {
        "localhost": {  
            "host-properties" :  {
             },
            "processes" :  {  
                "locator" : {
                      "type" : "locator"
                 },
                 "server1" : {
                    "type" : "datanode",
                    "server-port" : 10100
                 },
                 "server2" : {
                    "type" : "datanode",
                    "server-port" : 10200
                 }
            }
        }
   }
}

```

####Remote Cluster Configuration File####
```json
{
    "global-properties":{
        "gemfire": "/runtime/gemfire",
        "java-home" : "/runtime/java",
        "locators" : "10.0.0.101[10000]",
        "cluster-home" : "/runtime/cluster1"
    },
   "locator-properties" : {
        "port" : 10000,
        "jmx-manager-port" : 11099,
        "http-service-port" : 17070,
        "jmx-manager" : "true",
        "jmx-manager-start" : "true",
        "log-level" : "config",
        "statistic-sampling-enabled" : "true",
        "statistic-archive-file" : "locator.gfs",
        "log-file-size-limit" : "10",
        "log-disk-space-limit" : "100",
        "archive-file-size-limit" : "10",
        "archive-disk-space-limit" : "100",
        "jvm-options" : ["-Xmx8g","-Xms8g", "-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC"]
    },
   "datanode-properties" : {
        "server-port" : 10100,
        "conserve-sockets" : false,
        "log-level" : "config",
        "statistic-sampling-enabled" : "true",
        "statistic-archive-file" : "datanode.gfs",
        "log-file-size-limit" : "10",
        "log-disk-space-limit" : "100",
        "archive-file-size-limit" : "10",
        "archive-disk-space-limit" : "100",
        "jvm-options" : ["-Xmx12g","-Xms12g","-Xmn2g", "-XX:+UseConcMarkSweepGC", "-XX:+UseParNewGC", "-XX:CMSInitiatingOccupancyFraction=75"]
    },
    "hosts": {
        "hostA" : {
            "host-properties" :  {
            },
            "processes" : {
                "locator" : {
                    "type" : "locator",
                    "bind-address": "10.0.0.101",
                    "http-service-bind-address" : "10.0.0.101",
                    "jmx-manager-bind-address" : "10.0.0.101"
                }
            },
            "ssh" : {
                "host" : "52.91.207.138",
                "user" : "root",
                "key-file" : "my-keypair.pem"
             }
        },
        "hostB" : {
            "host-properties" :  {
            },
            "processes" : {
                "server111" : {
                    "type" : "datanode",
                    "bind-address": "10.0.0.111",
                    "server-bind-address" : "10.0.0.111"
                 }
            },
            "ssh" : {
                "host" : "54.83.157.253",
                "user" : "root",
                "key-file" : "my-keypair.pem"
            }
        }
    }
}
```
