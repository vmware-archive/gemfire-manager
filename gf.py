#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#
import clusterdef
import fileinput
import json
import os
import os.path
import io
import subprocess
import sys
import tempfile
import threading
import random


def determineExternalHost(ipaddress):

     #Determine ip address
    process = subprocess.Popen(["nslookup", ipaddress], stdout=subprocess.PIPE)
    output = str(process.communicate()[0])
    startEc2 = output.find("name = ec2-")
    startEc2 = startEc2+7
    endEc2 = output.find(".com",startEc2)+4

    externalHost = output[startEc2:endEc2]
    return externalHost

def runListQuietly(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = p.communicate()
    if p.returncode != 0:
        raise Exception('"{0}" failed with the following output: {1}'.format(' '.join(list(args)), output[0]))

def runQuietly(*args):
    runListQuietly(list(args))

def runRemote(sshInfo, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                     '-o','UserKnownHostsFile=/dev/null',
                        '-q',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]

    subprocess.check_call(prefix + list(args))

def runRemoteList(sshInfo, args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                     '-o','UserKnownHostsFile=/dev/null',
                        '-q',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]

    subprocess.check_call(prefix + args)


def launchRemote(sshInfo, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                     '-o','UserKnownHostsFile=/dev/null',
                        '-q',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]

    return subprocess.Popen(prefix + list(args))


def runRemoteQuietly(sshInfo, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                     '-o','UserKnownHostsFile=/dev/null',
                        '-q',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]

    runListQuietly( prefix + list(args))

def printUsage():
    print 'gf.py usage                                                #note, if cluster.json is in the same directory <clusterdef-file> may be ommitted'
    print '\tpython gf.py <clusterdef-file> gfsh                      #start an interactive gfsh session on a cluster member - connects automatically'
    print '\tpython gf.py <clusterdef-file> gfsh cmds...              #run a gfsh command (e.g. python gf.py gfsh list members) - connects automtically'
    print '\tpython gf.py <clusterdef-file> help                      #print this help message'
    print '\tpython gf.py <clusterdef-file> bounce                    #restart each cache server, one at a time, waiting for redundancy to be established before each stop'
    print '\tpython gf.py <clusterdef-file> start                     #start the whole cluster - idempotent - whatever is not started will be started'
    print '\tpython gf.py <clusterdef-file> stop                      #stop all cache servers (locators must be stopped explicitly) - idempotent'
    print '\tpython gf.py <clusterdef-file> start <process name>      #start a specific process'
    print '\tpython gf.py <clusterdef-file> stop <process name>       #stop a specific process (will not allow stopping a member when redundancy is not established)'
    print '\tpython gf.py <clusterdef-file> stop <process name> force #stop a member even if it would cause data loss'


def startCluster():
    # uses the clusterDef and cdef variables from global scope
    global clusterDef
    global cdef

    # with gem9 it seems that it is necessary to start locators in parallel
    # because they can wait on eachother if using the configuration service
    launches = []

    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]
        if 'processes' in host:
            for pkey in host['processes']:
                process = host['processes'][pkey]
                if process['type'] == 'locator':
                    #cluster path needs to be absolute
                    clusterScriptDir = cdef.locatorProperty(pkey,'cluster-home', host = hkey)

                    clusterScript = os.path.join(clusterScriptDir,'cluster.py')
                    launch = launchRemote(host['ssh'],'python', clusterScript,'start', pkey)
                    launches.append(launch)

    fails = 0
    for launch in launches:
        if launch.wait() != 0:
            fails += 1

    if fails > 0:
        sys.exit('at least one failure occurred while starting locators')
    else:
        print 'all locators started'


    # now start on all datanodes concurrently
    launches = []
    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]

        for pkey in host['processes']:
            process = host['processes'][pkey]
            if process['type'] == 'datanode':
                #cluster path needs to be absolute
                clusterScriptDir = cdef.datanodeProperty(pkey,'cluster-home', host = hkey)

                clusterScript = os.path.join(clusterScriptDir,'cluster.py')
                launch = launchRemote(host['ssh'],'python', clusterScript,'start', 'datanodes')
                launches.append(launch)
                break #BREAK - once you find one data node thats all you need

    fails = 0
    for launch in launches:
        if launch.wait() != 0:
            fails += 1

    if fails > 0:
        sys.exit('at least one failure occurred while starting cluster')
    else:
        print 'all datanodes started'

    # now start on all accessors concurrently
    launches = []
    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]

        for pkey in host['processes']:
            process = host['processes'][pkey]
            if process['type'] == 'accessor':
                #cluster path needs to be absolute
                clusterScriptDir = cdef.datanodeProperty(pkey,'cluster-home', host = hkey)

                clusterScript = os.path.join(clusterScriptDir,'cluster.py')
                launch = launchRemote(host['ssh'],'python', clusterScript,'start', 'accessors')
                launches.append(launch)
                break #BREAK - once you find one accessor thats all you need

    fails = 0
    for launch in launches:
        if launch.wait() != 0:
            fails += 1

    if fails > 0:
        sys.exit('at least one failure occurred while starting cluster')
    else:
        print 'cluster started'

    subprocess.call(['stty', 'sane'])


def runClusterScriptOnAnyHost(*args):
    # uses the clusterDef and cdef variables from global scope
    global clusterDef
    global cdef

    # pick an arbitrary process and use its cluster-home property to locate
    # the cluster.py script on that host
    success = False
    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]
        for pkey in host['processes']:
            if not success:
                process = host['processes'][pkey]
                if process['type'] == 'locator':
                    #cluster path needs to be absolute
                    clusterScriptDir = cdef.locatorProperty(pkey,'cluster-home', host = hkey)
                else:
                    clusterScriptDir = cdef.datanodeProperty(pkey,'cluster-home', host = hkey)

                clusterScript = os.path.join(clusterScriptDir,'cluster.py')

                # all of that was just to get the location of the remote script
                try :
                    print 'executing python {0} {1}  on {2}'.format(clusterScript, ' '.join(args),hkey)
                    runRemoteList(host['ssh'], ['python', clusterScript] + list(args) )
                    success = True
                except Exception as x:
                    print 'failed: {0}'.format(x)
                    pass

def runClusterScriptOnMemberHost(mname, *args):
    # uses the clusterDef and cdef variables from global scope
    global clusterDef
    global cdef

    # pick an arbitrary process and use its cluster-home property to locate
    # the cluster.py script on that host
    targetHost = None
    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]
        for pkey in host['processes']:
            if targetHost is None:
                if pkey == mname:
                    targetHost = host
                    process = host['processes'][pkey]
                    if process['type'] == 'locator':
                        #cluster path needs to be absolute
                        clusterScriptDir = cdef.locatorProperty(pkey,'cluster-home', host = hkey)
                    else:
                        clusterScriptDir = cdef.datanodeProperty(pkey,'cluster-home', host = hkey)


    if targetHost is None:
        sys.exit('no process "{0}" exists in the cluster definition'.format(mname))

    clusterScript = os.path.join(clusterScriptDir,'cluster.py')

    print 'executing python {0} {1} on {2}'.format(clusterScript, ' '.join(args),targetHost['ssh']['host'])
    runRemoteList(targetHost['ssh'], ['python',clusterScript] + list(args))

def redundancyEstablished(wait = 0):
    # uses the clusterDef and cdef variables from global scope
    global clusterDef
    global cdef

    # pick a random host to execute on
    targetHost = random.choice(list(clusterDef['hosts'].keys()))
    sshInfo = clusterDef['hosts'][targetHost]['ssh']

    #compile a list of locators
    locatorList = []
    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]
        for pkey in host['processes']:
            process = host['processes'][pkey]
            if process['type'] == 'locator':
                locatorList.append((hkey,pkey,host['ssh']))

    gemfire = getClusterProperty('gemfire', hostName = targetHost)
    javaHome = getClusterProperty('java-home', hostName = targetHost)
    clusterHome = getClusterProperty('cluster-home', hostName = targetHost)

    #try to connect to each jmx-manager in turn to identify an available one
    success = False
    for l in locatorList:
        try:
            host = cdef.locatorProperty(l[1],'jmx-manager-hostname-for-clients', host=l[0], notFoundOK = True)
            if host is None:
                host = cdef.locatorProperty(l[1],'jmx-manager-bind-address', host=l[0], notFoundOK=True)
                if host is None:
                    host = l[2]['host']

            port = cdef.locatorProperty(l[1],'jmx-manager-port', host=l[0], notFoundOK=True)
            if port is None:
                print 'warning, could not ascertain jmx-manager-port for "{0}" from settings - using 1099'.format(host)
                port = 1099

            p = subprocess.Popen(['ssh', '-o','StrictHostKeyChecking=no',
                        '-t', '-i', l[2]['key-file'],
                        '{0}@{1}'.format(l[2]['user'], l[2]['host']),
                        'GEMFIRE={0}'.format(gemfire), 'JAVA_HOME={0}'.format(javaHome),
                        os.path.join(gemfire, 'bin','gfsh'), '-e',
                        '"connect --jmx-manager={0}[{1}]"'.format(host,port)],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            p.communicate()
            if p.returncode == 0:
                print 'running redundancy check from {0} using jmx-manager host:port={1},{2}'.format(targetHost, host, port)
                cmd = ['ssh', '-o','StrictHostKeyChecking=no',
                            '-t', '-i', l[2]['key-file'],
                            '{0}@{1}'.format(l[2]['user'], l[2]['host']),
                            'GEMFIRE={0}'.format(gemfire), 'JAVA_HOME={0}'.format(javaHome),
                            '/usr/bin/python', os.path.join(clusterHome,'gemtools','checkred.py'),
                            '--jmx-manager-host={0}'.format(host),
                            '--jmx-manager-port={0}'.format(port)]
                if wait != 0:
                    cmd.append('--wait={0}'.format(wait))

                p = subprocess.Popen(cmd)
                p.wait()
                success = True
                if p.returncode == 0:
                    return True
                else:
                    return False
            else:
                print 'warning - could not connect to jmx manager "{0}"'.format(host)

        except Exception as x:
            pass

    if success == False:
        raise Exception('Unable to determine whether redundancy is established - exiting')


def stopMember(mname, force=False):
    if force or redundancyEstablished():
        runClusterScriptOnMemberHost(mname,'stop', mname)

def startMember(mname):
    runClusterScriptOnMemberHost(mname,'start', mname)

def stopCluster():
    runClusterScriptOnAnyHost('shutdown')

# TODO: should this be moved into clusterdef.py ?
#
# Looks up a property value
#
# If neither a process name nor host name is provided, an arbitrary host
# and process are selected and the the property value is returned for
# the arbitarily selected host and process
#
# If hostName is provided, an arbitrary process on that host is selected and
# the property value is returned for the given host and arbitrary process
#
# If processName is provided, hostName will be ignore because processName
# implies a host.  The property value for the given process is returned.
#
# If the provided host or process do not exist in the cluster definition, an
# exception is raised
#
# If the provided, or arbitrarily selected process exists but the property
# is not defined for that process, an exception is raised
#
def getClusterProperty(propName,  hostName = None, processName = None):
    # uses the clusterDef and cdef variables from global scope
    global clusterDef
    global cdef

    result = None
    if processName is None:
        if hostName is None:
            hostName = random.choice(list(clusterDef['hosts'].keys()))
        else:
            if hostName not in clusterDef['hosts']:
                raise Exception('host not found in cluster definition: ' + hostName)

        processName = random.choice(list(clusterDef['hosts'][hostName]['processes'].keys()))

    else:
        # the passed value of hostName is ignored (if provided) it is inferred
        # from the processName
        hostName = None
        for hkey in clusterDef['hosts']:
            host = clusterDef['hosts'][hkey]
            if processName in host['processes']:
                hostName = hkey

        if hostName is None:
            raise Exception('process name is not defined for any host: ' + processName)

    # at this point, hostName and processName are both defined and refer to
    # a valid host/process combination in the cluster definition
    process = clusterDef['hosts'][hostName]['processes'][processName]
    if process['type'] == 'locator':
        result = cdef.locatorProperty(processName,propName, host = hostName)
    else:
        result = cdef.datanodeProperty(processName,propName, host = hostName)

    return result

def gfsh(cmds = None):
    # uses the clusterDef and cdef variables from global scope
    global clusterDef
    global cdef

    # pick a random host to execute on
    targetHost = random.choice(list(clusterDef['hosts'].keys()))
    sshInfo = clusterDef['hosts'][targetHost]['ssh']

    locatorsProp = getClusterProperty('locators', hostName = targetHost)
    gemfire = getClusterProperty('gemfire', hostName = targetHost)
    javaHome = getClusterProperty('java-home', hostName = targetHost)
    clusterHome = getClusterProperty('cluster-home', hostName = targetHost)

    locators = locatorsProp.split(',')

    #look for a locator that is available
    availableLocator = None
    for locator in locators:
        p = subprocess.Popen(['ssh', '-o','StrictHostKeyChecking=no',
                        '-t', '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host']),
                        'GEMFIRE={0}'.format(gemfire) ,
                        'JAVA_HOME={0}'.format(javaHome),
                        os.path.join(gemfire,'bin','gfsh'),
                        '-e', '"connect --locator={0}"'.format(locator)],
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        p.communicate()
        if p.returncode == 0:
            availableLocator = locator
            break

    if availableLocator is None:
        raise Exception('No locators available in: ' + locatorsProp)

    if cmds is None:
        p = subprocess.Popen(['ssh', '-o','StrictHostKeyChecking=no',
                        '-t', '-t', '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host']),
                        'GEMFIRE={0}'.format(gemfire) ,
                        'JAVA_HOME={0}'.format(javaHome),
                        os.path.join(gemfire,'bin','gfsh')], stdin=subprocess.PIPE)

        p.stdin.write(bytes('connect --locator={0}\r'.format(availableLocator), encoding='ascii'))
        p.stdin.write(bytes('set variable --name=APP_RESULT_VIEWER --value=external\r', encoding='ascii'))
        p.poll()

        while p.returncode is None:
            line = input()
            p.stdin.write(bytes(line + '\r', encoding='ascii'))
            if line == 'quit':
                p.stdin.close()
                p.communicate()
                break

            p.poll()
    else:
        p = subprocess.Popen(['ssh', '-o','StrictHostKeyChecking=no',
                        '-t', '-t', '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host']),
                        'GEMFIRE={0}'.format(gemfire) ,
                        'JAVA_HOME={0}'.format(javaHome),
                        os.path.join(gemfire,'bin','gfsh'),
                        '-e', '"connect --locator={0}"'.format(availableLocator),
                        '-e', '"{0}"'.format(' '.join(cmds))])

        p.wait()


def bounce():
    global clusterDef

    for hkey in clusterDef['hosts']:
        for pkey in clusterDef['hosts'][hkey]['processes']:
            process = clusterDef['hosts'][hkey]['processes'][pkey]
            if process['type'] == 'datanode':
                if not redundancyEstablished():
                    print 'witing up to 5 minutes for redundancy to be established'
                    if not redundancyEstablished(300):
                        raise Exception('redundancy could not be established - exiting')

                stopMember(pkey, force = True)
                startMember(pkey)

    print 'all datanodes bounced'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        printUsage()

    here = os.path.dirname(sys.argv[0])

    if os.path.exists('cluster.json'):
        clusterDefFile = 'cluster.json'
        cmdOffset = 0
    else:
        clusterDefFile = sys.argv[1]
        if not os.path.exists(clusterDefFile):
            sys.exit('cluster definition file not found: ' + clusterDefFile)

        cmdOffset = 1

    ## TODO
    # This block should be refactored.  It should not be necessary to
    # have both cdef (the ClusterDef object) and clusterDef (the raw JSON object)
    #
    cdef = clusterdef.ClusterDef(clusterDefFile)

    #not doing environment variable substitutions because
    #this script only runs commands on remote servers
    with open(clusterDefFile,'r') as cdfile:
        clusterDef = json.load(cdfile)
        if 'dummy' in clusterDef['hosts']:
            del clusterDef['hosts']['dummy']


    print 'loaded cluster defintion from {0}'.format(clusterDefFile)
    #########################################

    if len(sys.argv) == 2 + cmdOffset:
        cmd = sys.argv[1 + cmdOffset]
        if cmd == 'start':
            startCluster()
        elif cmd == 'stop':
            stopCluster()
        elif cmd == 'help':
            printUsage()
        elif cmd == 'bounce':
            bounce()
        elif cmd == 'gfsh':
            gfsh()
        else:
            sys.exit('an unrecognized command was supplied: {0}'.format(cmd))
    else:
        cmd = sys.argv[1 + cmdOffset]
        target = sys.argv[2 + cmdOffset]
        if cmd == 'start':
            startMember(target)
        elif cmd == 'stop':
            if len(sys.argv) == 4 + cmdOffset and sys.argv[3 + cmdOffset] == 'force':
                stopMember(target, force = True)
            else:
                stopMember(target)
        elif cmd == 'gfsh':
            gfsh(sys.argv[2 + cmdOffset:])
        else:
            sys.exit('an unrecognized command was supplied: {0}'.format(cmd))
