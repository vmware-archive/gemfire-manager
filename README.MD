#Overview#

The purpose of this project is to simplify GemFire cluster management by adding
the cluster level functionality that GemFire users usually must write themselves
. For example:  parallel cluster startup, rolling bounce, proper shutdown.  It
also solves common configuration management issues by consolidating all
configurtion to a single file which is copied to all managed machines.

Each managed host must meet the following prerequisites:
* Linux operating system
* SSH and rsync installed
* There must be GemFire "run as" user on the machine and that user must be
configured for passwordless SSH
* An appropriate java JDK (not JVM) must be installed
* Python 2.6+
* the "netifaces" python package (usually can be installed with
`pip install netifaces`)

It is also a convenient way to manage local clusters (in which case no SSH setup
is required).  

#Local Cluster Walk Through#






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

