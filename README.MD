#Overview#

The purpose of this project is to simplify GemFire cluster management by adding
the cluster level functionality that GemFire users usually must write themselves
.In a nutshell, it adds commands to:
* start/stop a whole cluster
* start/stop individual members (whether local or remote)
* prevent a user from stopping a member when it could cause data loss.
* correctly perform a rolling restart (waiting for redundancy to be established when necessary) 

#Requirements#

Each managed host must meet the following prerequisites:
* Linux or OSX operating system 
* SSH and rsync installed (not required for local clusters)
* The user account that GemFire will run under must be configured for passwordless SSH
(not required for local clusters).  
* An appropriate java JDK (not JVM) must be installed
* An installation of GemFire (8.x or higher)
* Python 2.6+
* The following packages need to be installed: awscli, gcc, python-devel,python-pip
* the "netifaces" python package.  This usually can be installed as follows:
`pip install netifaces`.  


#Local Cluster Walk Through#

1. Define the GEMFIRE and JAVA_HOME environment variables to point to your
gemfire installation and an appropriate jdk.
2. Examine the cluster configuration file: _samples/local-cluster.json_.
3. Copy _samples/local-cluster.json_ to _cluster.json_ in the project root directory (i.e. the
directory where _cluster.py_ resides)
3. If you wish to change the location used by the cluster for local storage,
you may do so by modifying the _cluster-home_ setting in _cluster.json_.
4. Start the cluster:
```
    python cluster.py start
```
5. Verify the cluster is running by accessing pulse at: http://localhost:17070/pulse
6. Note: the locator is listening on port 10000, the jmx-manager is on port 11099
7. Check the status of a member:
```
    python cluster.py status server1
```
Note that the member name is determined by its key in the _processes_ dictionary
under each host.
8. stop and start a member
```
    python cluster.py stop server2
    python cluster.py start server2
```
9. stop the whole cluster
```
    python cluster.py stop
```
Note that this does not stop locators.
10. stop the locator
```
    python cluster.py stop locator
```
    
    
# Setting Up a Remote Cluster #

GemFire manager has certain requirements about how managed members are set up.

* hosts must have a gemfire installation and a JDK and the _gemfire_ and _java-home_
properties in _cluster.json_ must point to those locations.
* cluster scripts _cluster.py, clusterdef.py and gemprops.py_ must be placed in 
the cluster home directory (which is specified using the _cluster-home_ setting).
* All machines must have the same cluster definition file.  This file must be named
_cluster.json_ and it must be in the same directory as the cluster scripts.
* The machines hosting cluster members must be set up with a user who will run the
admin commands (e.g. start and stop servers). That user must be set up for
passwordless ssh logins
* The admin user must have write access to the cluster-home directory and the
configuration files and scripts it contains.


#Remote Cluster Walkthrough#
This walk though takes you through setting up and starting a gemfire cluster
with 1 locator and 3 data nodes.  You will be able to control the cluster from
your local machine provided you have ssh access to all 4 remote machines.

1. Provision or obtain access to 4 servers.  Ensure that java and gemfire have been copied
to the same location on all for servers (the sample script assumes that java
and gemfire are in _/runtime/java_ and _/runtime/gemfire_ respectively).

2. Set up all 4 machines for passwordless ssh. You will of course need the
private key file on your local machine.

3. Create a cluster home directory in the same place on all 4 machines
(e.g. /runtime/cluster_1)

4. Copy _samples/remote-cluster.json_ to  _cluster.json_ in the project
home directory (the directory where _cluster.py_ is).

5. Edit _cluster.json_ - set the _cluster-home_,_java_home_ and _gemfire_ settings
to the directories you chose in steps 1-3.
 
2. Start the cluster.
```
python gf.py start
```
3. Verify the cluster is running by checking pulse. Assuming you have a gemfire
manager running on host "locatorhost" the url would be: http://locatorhost:17070/pulse
Log in with "admin"/"admin"
4. Stop and start a member.  Use the member name, which is the key of the process
entry in the cluster.json file.
```
python gf.py stop server111
python gf.py start server111
```
5. Stop the whole cluster (except locators)
```
python gf.py stop
```
6. Stop the locator (assuming the member name is "locator")
```
python gf.py stop locator
```
    
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
* _bind-address_ can be an interface name, for example "eth0".
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


