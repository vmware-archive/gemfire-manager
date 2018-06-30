# Overview

The purpose of this project is to simplify GemFire cluster management by adding
the cluster level functionality that GemFire users usually must write themselves
.In a nutshell, it adds commands to:

* install a gemfire cluster
* start/stop a whole cluster
* start/stop individual members
* prevent a user from stopping a member when it could cause data loss.
* correctly perform a rolling restart by waiting for redundancy to be
  established when necessary.

__This project is not intended to replace gfsh.__  gfsh is the recommended
approach for most administrative tasks like creating regions or adding
indices. 

Most of these tasks require starting a GemFire process on a remote machine,
which is something gfsh does not do.  This project is meant to automate the
error-prone and tedious practice of logging in to all of the machines
in a cluster to make configuration changes or start processes.

# How it works

All configuration for the whole gemfire cluster resides in a single
configuration file, _cluster.json_, which can be versioned.

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



# Requirements

Each managed host must meet the following prerequisites:

* Linux or OSX operating system
* The GemFire 9.x product must be installed somewhere on the file system
(readable by the run as user)
* A compatible Java 1.8 JDK
* Python 2.6+

The local machine which will be used to perform the installation needs
the following:

* A Java 1.8 JDK
* A working Apache Maven install
* Python 2.6+


# Walk Through

This walk through illustrates how to install and manage a cluster on a pair of bare bones Centos 7 VMs. For convenience, a Vagrant dev cluster configuration
is provided which you can use or you can use any other linux VMs.

After cloning the project, you need to get all submodules as well.

```
git submodule init
git submodule update
```

Build the people-loader project.

```
cd people-loader
mvn package
cd ..
```

Now run _setup.py_ in the _vagrant_ folder.  This will download a copy of GemFire which will be installed on the Vagrant machines.  It will also generate a keystore which will be used to encrypt the
connections using SSL

```
cd vagrant
python setup.py
```

Now bring the vagrant machines up.

```
vagrant up
```

While they are being initialized, peruse _Vagrantfile_ to see how the boxes are being configured.  After provisioning, the machines will have:

* a JDK installed
* Naming configured such that `hostname` on each machine is resolvable on every other machine in the cluster
* Passwordless SSH is configured for the run as user, which in this
case is _vagrant_
* GemFire is unzipped to a location readable by the run as user.
* Directories are set up for the cluster working directory, logs, data stores (if any) and backups, all writeable by the run as user.
* If SSL will be used, the keystore and _security.properties_ file
(which contains passwords) are readable by the run as user but otherwise locked down.

_This represents a typical starting point for installing a GemFire cluster on-premise and the same requirements would apply._

Now have a look at _cluster.json_ in the _vagrant_ folder.  Note that settings that are the same for all servers are specified only once at the global level while settings that are specific to each machine are in a machine specific section.  Below are a few important points about this file. See _The Cluster Configuration File_ below for more details.

* Each host must have a section and the name of the section must
match the value returned by `hostname` on that machine.
* Each host section contains an _ssh_ section that contains a name
or IP address, a user name, and the location of the key file to use
for passwordless ssh. The user name must be the run as user.

Here is an example host configuration section.

```
"server2" : {
    "host-properties" :  {
     },
     "processes" : {
        "datanode2" : {
            "type" : "datanode",
            "bind-address" : "10.10.10.102",
            "http-service-port": 18080,
            "start-dev-rest-api" : "true"
         }
     },
     "ssh" : {
        "host" : "10.10.10.102",
        "user" : "vagrant",
        "key-file" : "vagrant/id_rsa"
     }
}
```
Now install the cluster.  This does the following:

* builds and uploads _gemfire_toolkit_, which contains a collection of helper functions used by the framework.
* places the cluster configuration file in the cluster home directory
* copies some cluster control scripts to the cluster home directory


```
cd ..
python installcluster.py vagrant/cluster.json
```

If the cluster configuration, the scripts or the toolkit need to be
modified, this script can safely be run again to push the updates
onto all machines in the cluster.

Now you can start the cluster.

```
python gf.py vagrant/cluster.json start
```

This command will ssh onto each machine in the cluster and execute the
proper gfsh commands to start the cluster with SSL enabled for intra-cluster
traffic and client-server traffic.

Verify that the cluster is running:

```
python gf.py vagrant/cluster.json gfsh list members
```

This command executes a gfsh command on an available server in the cluster.  The
gf.py script takes care of the `gfsh connect` command for you.  You can use this technique to run any gfsh command on the cluster.

It is recommended to configure _read-serialized=true_ for GemFire clusters where
there will be no server side code.  With _read-serialized=true_ it is not
necessary to put domain jars on the server class path.  However, this is not
the default.  __This setting should be configured as part of cluster
initialization and a cluster restart is required after it is changed.__  
For this example, initialize pdx using the following commands.

```
export GEMFIRE=/usr/local/Cellar/gemfire/9.3.0/libexec  # location of your local GemFire
$GEMFIRE/bin/gfsh 

# gfsh session below
gfsh> connect --locator=10.10.10.101[10000]
Connecting to Locator at [host=10.10.10.101, port=10000] ..
Connecting to Manager at [host=10.10.10.101, port=11099] ..
Successfully connected to: [host=10.10.10.101, port=11099]

Cluster-1 gfsh>create disk-store --name=pdx-disk-store --dir=data/pdx
 Member   | Status
--------- | -------
datanode1 | Success
datanode2 | Success

Cluster-1 gfsh>configure pdx --disk-store=pdx-disk-store --read-serialized=true
The command would only take effect on new data members joining the distributed system. It won't affect the existing data members
persistent = true
disk-store = pdx-disk-store
read-serialized = true
ignore-unread-fields = false

Cluster-1 gfsh>shutdown
As a lot of data in memory will be lost, including possibly events in queues, do you really want to shutdown the entire distributed system? (Y/n): Y
Shutdown is triggered

Cluster-1 gfsh>quit 

# now the cluster is stopped so start it again
python gf.py vagrant/cluster.json start 
```

Now we will create a region on the remote cluster and load some data using the
_people-loader_ sample application.

```
python gf.py vagrant/cluster.json gfsh create disk-store --name=person-disk-store --dir=/home/vagrant/gemfire_cluster/data/person
python gf.py vagrant/cluster.json gfsh create region --name=Person --disk-store=person-disk-store --type=PARTITION_REDUNDANT_PERSISTENT
python vagrant/peopleloader.py --locator=10.10.10.101[10000] --count=10000
```

Review _vagrant/peopleloader.py_ to see how SSL is configured for a client.  
You can see that the following options are set by the script:

```
-Dgemfire.ssl-keystore=vagrant/trusted.keystore
-Dgemfire.ssl-keystore-password=password
-Dgemfire.ssl-truststore=vagrant/trusted.keystore
-Dgemfire.ssl-truststore-password=password
-Dgemfire.ssl-default-alias=self
-Dgemfire.ssl-enabled-components=server
-Djavax.net.ssl.keyStoreType=JKS
-Djavax.net.ssl.trustStoreType=JKS
```

To finish up, use a gfsh _get_ command to verify the data is there.  

```
python gf.py vagrant/cluster.json gfsh get --region=Person --key=1 --key-class=java.lang.Integer
```

To stop the whole cluster:

```
python gf.py vagrant/cluster.json gfsh shutdown --include-locators=true
```

# Local Cluster Walk Through

This framework can also be used to quickly set up a local cluster with whatever
configuration you desire.  It uses the same _cluster.json_ cluster configuration
file. However, instead of using _gf.py_, _cluster.py_ is used with local
clusters.  _gf.py_ always runs commands via ssh whereas _cluster.py_ always acts locally.

Define the GEMFIRE and JAVA_HOME environment variables to point to your
gemfire installation and an appropriate jdk.

First, __copy local-cluster.json to cluster.json__

Now examine _cluster.json_ and make any changes you might wish to make.  By 
default, the local cluster will be created in the same directory as this 
project.

Now start the cluster.

```
python cluster.py start
```

Now, as with the remote cluster, create a disk store and configure pdx read
serialized=true and restart the cluster. However this time you can use gfsh
directly.

```
$GEMFIRE/bin/gfsh -e "connect --locator=localhost[10000]" -e "create disk-store --dir=data/pdx --name=pdx-disk-store" -e "configure pdx --disk-store=pdx-disk-store --read-serialized=true"
$GEMFIRE/bin/gfsh -e "connect --locator=localhost[10000]" -e "shutdown"
python cluster.py --cluster-def=local/cluster.json start
```

Now we can create a persistent region.

```
$GEMFIRE/bin/gfsh -e "connect --locator=localhost[10000]" -e "create disk-store --dir=data/person --name=person-disk-store"
$GEMFIRE/bin/gfsh -e "connect --locator=localhost[10000]" -e "create region --name=Person --disk-store=person-disk-store --type=PARTITION_REDUNDANT_PERSISTENT"
```

... and load some data (note there is a different _peopleloader.py_ script
because SSL is not configured in the local example).

```
python people-loader/peopleloader.py --locator=localhost[10000] --count=10000
```

... and retrieve the data using a query

```
$GEMFIRE/bin/gfsh -e "connect --locator=localhost[10000]" -e "query --query='select * from /Person'"
```

... and finally stop the cluster

```
$GEMFIRE/bin/gfsh -e "connect --locator=localhost[10000]" -e "shutdown --include-locators=true"
```

# The Cluster Configuration File

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

#### Additional notes about the cluster configuration file 

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

#### Local Cluster Configuration File 

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

####Remote Cluster Configuration File 

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
