#Overview#

The purpose of this project is to simplify GemFire cluster management by adding
the cluster level functionality that GemFire users usually must write themselves
.In a nutshell, it adds commands to:
* start/stop a whole cluster
* start/stop individual members (whether local or remote)
* correctly perform a rolling restart (waiting for redundancy to be established when necessary)

#Requirements#

Each managed host must meet the following prerequisites:
* Linux or OSX operating system 
* SSH and rsync installed (not required for local clusters)
* The user account that GemFire will run under must be configured for
passwordless SSH (not required for local clusters).  
* An appropriate java JDK (not JVM) must be installed
* An installation of GemFire (8.x or higher)
* Python 2.6+
* the "netifaces" python package.  This usually can be installed as follows:
`pip install netifaces`.  If you don't have pip, try installing it with your
package manager (e.g. `sudo apt-get install python-pip`). Finally, if that
doesn't work, you can follow the directions here: https://pip.pypa.io/en/stable/installing.
Once you have pip installed, simply type `pip install netifaces` to install the python package.

#Operating Principles#
* All commands are idempotent 

#Setting Up a Cluster Config Directory#
gemfire\_manager prescribes that all cluster configuration be in a single directory
with the structure described below.  This directory contains all configuration
and code required to install and start a gemfire cluster member on a remote machine.
_Whenever a member is started on a remote system, gemfire_manager will check for
changes and if necessary push the changes to the remote system before starting
the member._  The directory strucure for a cluster configuration directory is:

```
<cluster-home>
|-gemfire // directory - this is the full gemfire release
|-lib // directory - additional jars go here 
|-cluster.json // cluster definition file
```

Need to talk about the cluster master directory vs. the config directory
vs the working directory vs logs vs disk stores

#Local Cluster Walk Through#

In this walk through, 

1. Examine the cluster configuration file: _samples/local-cluster/cluster.json_.
This file contains all of the configuration for starting and managing a GemFire
cluster.  It includes GemFire and JVM settings but does not define regions or
anything else that would traditionally reside in "cache.xml".

    The file has a hierarchical structure which eliminates most cases of redundant
    configuration by allowing settings common to all cluster members in a global
    area.  In fact, there are several shared sections.  When a datanode/locator
    is started, settings are looked up in the following order:
    
    * __host and process specific__ (see for example the _server-port_ settings in
    the sample below)
    * if there are host and process specific settings, check __host__ settings ( the
    sample has not setting at the host level but they would be found inside of
    _hosts["localhost"]["host_properties"]_ )
    * if there are no host specific settings, check the __global settings for the
    process type__. (In the example, _conserve_sockets_ is set at this level).
    There is one section that applies to all data nodes (_datanode-properties_),
    and one that applies to locators (_locator-properties_).
    * Lastly, look in _global-properties_
    
2. x
    
    
    
# Example Cluster Configuration File#

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

##About the Cluster Definiton File##





On each host, the following directory structure is created


```
    <cluster-home>
    |- gemfire
    |- config
    |- lib
```

gemfire-version or gemfire
java-version or java-home

#Pre-requisites#
* The managed machines must be linux and must support SSH and rsync.
* They must have a user account on them for use by gemfire and the account must
   be s


Quick start

Distributed Cluster Management


#Areas for Further Work#
1. Auto-provisioning of a specific gemfire version / jvm version
2. Cluster configuration wizard
3. Web UI

#Story 1#
You start some servers and put the desired version of GemFire and java
on them. Run the start cluster command.  Each process if first checked to
see if it is already running.  If it is, that is reported, if not, the local
image is updated and the process is started.  The processes are started
in parallel. As each process starts sucessfully, it is reported. Once all
have started, a cluster status message is displayed.  If not all have started
after a reasonable amount of time, a report is printed.

