#Overview#

The purpose of this project is to simplify GemFire cluster management by adding
the cluster level functionality that GemFire users usually must write themselves
.In a nutshell, it adds commands to:
* start/stop a whole cluster
* start/stop individual members (whether local or remote)
* __manage remote members__ (e.g. start a whole cluster without logging in to each member)
* correctly perform a rolling restart (waiting for redundancy to be established when necessary) - COMING SOON

#Requirements#

Each managed host must meet the following prerequisites:
* Linux or OSX operating system 
* SSH and rsync installed (not required for local clusters)
* The user account that GemFire will run under must be configured for
passwordless SSH (not required for local clusters).  
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
3. If you wish to change the location used by the cluster for local storage,
you may do so by modifying the _cluster-home_ setting.
4. Start the cluster:
```
    python cluster.py --cluster-def=samples/local-cluster.json start
```
5. Verify the cluster is running by accessing pulse at: http://localhost:17070/pulse
6. Note: the locator is listening on port 10000, the jmx-manager is on port 11099
7. Check the status of a member:
```
    python cluster.py --cluster-def=samples/local-cluster.json status server1
```
Note that the member name is determined by its key in the _processes_ dictionary
under each host.
8. stop and start a member
```
    python cluster.py --cluster-def=samples/local-cluster.json stop server2
    python cluster.py --cluster-def=samples/local-cluster.json start server2
```
9. stop the whole cluster
```
    python cluster.py --cluster-def=samples/local-cluster.json stop
```
Note that this does not stop locators.
10. stop the locator
```
    python cluster.py --cluster-def=samples/local-cluster.json stop locator
```
    
####Additional Notes####
* If the _--cluster-def_ setting is ommitted, the script will look for a file
named _cluster.json_ in the same directory as the script.
    
# Setting Up a Remote Cluster #

GemFire manager has certain requirements about how managed members are set up.
A utility is provided to assist with this but it can also be done manually.
The requirements are as follows:

* hosts must have a gemfire installation and a JDK and the _gemfire_ and _java-home_
properties in _cluster.json_ must point to those locations.
* cluster scripts _cluster.py, clusterdef.py and gemprops.py_ must be placed in
the parent directory of the cluster home directory (which is specified using the _cluster-home_ setting).
* All machines must have the same cluster definition file.  This file must be named
_cluster.json_ and it must be in the same directory as the cluster scripts.
* The cluster home directory, the cluster scripts and the cluster definition file
must be accessible by a user that has _passwordless_ ssh access to the machine.
This should be configured by placing the administrators public key in the users
~/.ssh/authorized_keys file.
* The machine which will be used for admin does not have to be a cluster member
but it must have ssh access to all cluster members.  Specifically, the admin
machine must have the .pem file on it.

To set up a cluster using the provided install script, follow these instructions:
1. Provision or obtain access to a group of linux servers.  Ensure all of the
servers are set up with passwordless ssh and that you have the corresponding
".pem" file on your local machine.
2. Review the "Requirements" section above.  Make sure all necessary packages
have been installed. _The utility will install GemFire and Java for you so you
can skip that part._
2. Copy the whole gemfire-manager folder to another folder (hereafter called
~/mycluster ) that you will use to manage this cluster. 
3. Place you .pem file into ~/mycluster
4. Copy the "remote-cluster.json" from the gemfire-manager/samples folder
into the new folder.  Steps 3-5 would look something like this:
```
cp -r ~/gemfire-manager ~/mycluster
cp ~/mykey.pem ~/mycluster
cp ~/mycluster/samples/remote-cluster.json ~/mycluster/cluster.json
```
5. Edit mycluster/cluster.json.  At a minimum, you will need to be sure that
the "hosts" dictionary contains an entry for each server in the cluster.  The
key to the dictionary entry must be the server's host name as returned by the
"hostname" command on that server.  Further, each host entry must contain
an "ssh" entry containing the "host", "user" and "key-file" to be used when
access the server with ssh.  The example below shows a minimal cluster.json that
includes 2 servers.
6. Run `python install.py` in the "mycluster" directory.  This will do the the
following:
    * copy cluster.json and all of the cluster control scripts into the parent
directory of directory specified by the "cluster-home" property.  In the example
below, this would be "/runtime/cluster1"
    * install gemfire 8.2 in the directory specified by the "gemfire" property
    * install a compatible Oracle 1.8 JDK in the directory specified by the
    "java-home" property
    
At this point, if all goes well, you will have a gemfire cluster set up and
ready to run.

#Remote Cluster Walkthrough
1. Complete the setup steps above.
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
        "cluster-home" : "/runtime/cluster1/var"
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


