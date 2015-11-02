#Overview#

The purpose of this project is to simplify GemFire cluster management and
provide basic cluster control functions that are not provided by GemFire
out of the box. 


On each host, the following directory structure is created


```
    <cluster-home>
    |- gemfire
    |- java
    |- config
    |- lib
```

gemfire-version or gemfire
java-version or java-home

#Pre-requisites#
The managed machines must be linux and must support SSH and rsync.  They
must have a user account on them for use by gemfire.


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

