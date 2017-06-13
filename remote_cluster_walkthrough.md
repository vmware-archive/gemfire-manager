#Remote Cluster Walkthrough#
This walk though takes you through setting up and starting a gemfire cluster
with 1 locator and 3 data nodes.  You will be able to control the cluster from
your local machine provided you have ssh access to all remote machines.

1. Provision or obtain access to 4 servers. Review the requirements above and
and ensure that all of the required packages have been installed.

2. Place an unpacked JDK and GemFire installation in the same location on all
servers (the sample script assumes that java and gemfire are in _/runtime/java_
and _/runtime/gemfire_ respectively).

2. Set up all 4 machines for passwordless ssh. You will of course need the
private key file on your local machine.

3. Create a cluster home directory in the same place on all 4 machines
(e.g. /runtime/cluster_1)

4. On your local machine, copy _samples/remote-cluster.json_ to  _cluster.json_
in the project home directory (the directory where _cluster.py_ is).

5. Edit the _global_properties_ section of _cluster.json_. Set the
_cluster-home_,_java_home_ and _gemfire_ settings to the directories you chose
in steps 1-3.

6. Decide which machine(s) will host a locator.  Edit  the _global_properties_
section of _cluster.json_ and set the _locators_ property accordingly.  The code
block below is an example of how the _global_properties_ should look.

    ```json
    {
        "global-properties":{
            "gemfire": "/runtime/gemfire",
            "java-home" : "/runtime/java",
            "locators" : "locator.example.com[10000]",
            "cluster-home" : "/runtime/cluster1"
    },
    ...
    ```
7. Edit _cluster.json_.  Under the entry for each host, configure the ssh user,
host and keypair.  This information will be used by the _gf.py_ cluster control
script to access the members of the cluster.  The _key_file_ setting must point
to the key file on your local machine.  The members of the cluster do not need
SSH access to eachother.

    ```json
    ...
   "hosts": {
        "locator.example.com" : {
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
                "host" : "54.236.255.190",
                "user" : "root",
                "key-file" : "/home/me/.ssh/id_rsa"
             }
        },
        ...
    ```

8. Copy the contents of the project directory on your local machine into the
cluster home directory (as specified in the _global_properties_ section of
_cluster.json_ ) on all of the remote machines.  The _cluster_home_ directories on
all remote machines should now contain the recently edited _cluster.json_ as well
as the cluster control scripts: _gf.py_, _cluster.py_, _clusterdef.py_ and _gemprops.py_.

9. Start the remote cluster using the followng command from the local machine.
    ```
    python gf.py start
    ```
    This will start any cluster members that are not already started.  It will
    start all data nodes in parallel to avoid deadlock during startup.
3. Verify the cluster is running by checking pulse. Assuming you have a gemfire
manager running on host "locator.example.com" the url would be:
http://locator.example.com:17070/pulse.  You can with username/password ="admin"/"admin"

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
