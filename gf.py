import clusterdef
import json
import os
import os.path
import subprocess
import sys


def runListQuietly(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = p.communicate()
    if p.returncode != 0:
        raise Exception('"{0}" failed with the following output: {1}'.format(' '.join(list(args)), output[0]))
    
def runQuietly(*args):
    runListQuietly(list(args))
    
def runRemote(sshInfo, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]
    
    subprocess.check_call(prefix + list(args))

def runRemoteList(sshInfo, args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]
    
    subprocess.check_call(prefix + args)

    
def launchRemote(sshInfo, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]
    
    return subprocess.Popen(prefix + list(args))

    
def runRemoteQuietly(sshInfo, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                        '-t',
                        '-i', sshInfo['key-file'],
                        '{0}@{1}'.format(sshInfo['user'], sshInfo['host'])]
    
    runListQuietly( prefix + list(args))

def printUsage():
    print 'gf.py usage (cluster.json must be in same directory)'
    print '\tpython gf.py start'
    print '\tpython gf.py stop'
    print '\tpython gf.py restart'
    print '\tpython gf.py start <process name>'
    print '\tpython gf.py stop <process name>'
    print '\tpython gf.py list members'


def startCluster():
    # uses the clusterDef and cdef variables from global scope
    global clusterDef
    global cdef
    
    # start locators first
    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]
        if 'processes' in host:
            for pkey in host['processes']:
                process = host['processes'][pkey]
                if process['type'] == 'locator':
                    #cluster path needs to be absolute
                    clusterScriptDir = cdef.locatorProperty(pkey,'cluster-home', host = hkey)

                    clusterScript = os.path.join(clusterScriptDir,'cluster.py')
                    runRemote(host['ssh'],'python', clusterScript, 'start' , pkey)
                
    # now start on all servers concurrently
    launches = []
    for hkey in clusterDef['hosts']:
        host = clusterDef['hosts'][hkey]
        
        for pkey in host['processes']:
            process = host['processes'][pkey]
            if process['type'] == 'datanode':
                #cluster path needs to be absolute
                clusterScriptDir = cdef.datanodeProperty(pkey,'cluster-home', host = hkey)

                clusterScript = os.path.join(clusterScriptDir,'cluster.py')
                launch = launchRemote(host['ssh'],'python', clusterScript,'start')
                launches.append(launch)
                continue #CONTINUE - once you find one data node thats all you need
    
    fails = 0
    for launch in launches:
        if launch.wait() != 0:
            fails += 1
            
    if fails > 0:
        sys.exit('at least one failure occurred while starting cluster')
    else:
        print 'all servers started'
        

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
                    runRemoteList(host['ssh'], [clusterScript] + list(args) )
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
    runRemoteList(targetHost['ssh'], [clusterScript] + list(args))

def stopMember(mname):
    runClusterScriptOnMemberHost(mname,'stop', mname)

def startMember(mname):
    runClusterScriptOnMemberHost(mname,'start', mname)
    
def stopCluster():
    runClusterScriptOnAnyHost('stop')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        printUsage()
        sys.exit('no command supplied')
    
    here = os.path.dirname(sys.argv[0])
    clusterDefFile = os.path.join(here, 'cluster.json')
    if not os.path.isfile(clusterDefFile):
        sys.exit('could not find cluster definition file: ' + clusterDefFile)
        
    #not doing environment variable substitutions because
    #this script only runs commands on remote servers
    with open(clusterDefFile,'r') as cdfile:
        clusterDef = json.load(cdfile)
        cdef = clusterdef.ClusterDef(clusterDef)
        
    print 'loaded cluster defintion from {0}'.format(clusterDefFile)

    cmd = sys.argv[1]
    
    if len(sys.argv) == 2:
        if cmd == 'start':
            startCluster()
        elif cmd == 'stop':
            stopCluster()
        else:
            sys.exit('an unrecognized command was supplied: {0}'.format(cmd))
    else:
        target = sys.argv[2]
        if cmd == 'start':
            startMember(target)
        elif cmd == 'stop':
            stopMember(target)
        else:
            sys.exit('an unrecognized command was supplied: {0}'.format(cmd))
        
        
        