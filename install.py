import clusterdef
import json
import os
import os.path
import subprocess
import sys

#args should be a list
def runListQuietly(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = p.communicate()
    if p.returncode != 0:
        raise Exception('"{0}" failed with the following output: {1}'.format(' '.join(list(args)), output[0]))
    
def runQuietly(*args):
    runListQuietly(list(args))
    
def runRemote(sshKeyPath, user, host, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                        '-t',
                        '-i', sshKeyPath,
                        '{0}@{1}'.format(user, host)]
    
    subprocess.check_call(prefix + list(args))
    
    
def runRemoteQuietly(sshKeyPath, user, host, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                        '-t',
                        '-i', sshKeyPath,
                        '{0}@{1}'.format(user, host)]
    
    runListQuietly( prefix + list(args))
    
    
if __name__ == '__main__':
    here = os.path.dirname(sys.argv[0])
    if len(here) == 0:
        here = '.'
        
    #read cluster.json 
    with open(os.path.join(here,'cluster.json'), 'r') as contextFile:
        clusterDefRaw = json.load(contextFile)
    
    clusterDef = clusterdef.ClusterDef(clusterDefRaw)
    
    for hkey in clusterDefRaw['hosts']:
        host = clusterDefRaw['hosts'][hkey]
        # pick the first process and use it to look up the gemfire and java-home settings
        gemfire = None
        javaHome = None
        clusterHome = None
        for pkey in host['processes']:
            process = host['processes'][pkey]
            gemfire = clusterDef.processProperty(process['type'],pkey,'gemfire', host=hkey)
            javaHome = clusterDef.processProperty(process['type'],pkey,'java-home', host=hkey)
            clusterHome = clusterDef.processProperty(process['type'],pkey,'cluster-home', host=hkey)
            if gemfire is not None and javaHome is not None and clusterHome is not None:
                break
            
        if gemfire is None or javaHome is None or clusterHome is None:
            sys.exit('could not look up gemfire, java-home and cluster-home settings for host {0}'.format(hkey))

        #copy the scripts and cluster.json to the parent of the cluster-home directory
        clusterParent = os.path.dirname(clusterHome)
        keyFile = host['ssh']['key-file']
        hostName = host['ssh']['host']
        userName = host['ssh']['user']
        if not os.path.isfile(keyFile):
            sys.exit('key file {0} not found'.format(keyFile))
        
        # create the clusterParent dir on the remote host if it does not exist
        runQuietly('scp','-o','StrictHostKeyChecking=no','-i', keyFile, os.path.join(here, 'ensuredirs.py'), '{0}@{1}:/tmp'.format(userName, hostName, clusterParent))
        runRemote(keyFile, userName, hostName, 'python', '/tmp/ensuredirs.py', clusterParent)        
        
        sources = ['cluster.py', 'clusterdef.py','gemprops.py','gf.py','cluster.json', 'installgem.py','installjava.py']
        for s in sources:
            source = os.path.join(here, s)
            runQuietly('scp','-o','StrictHostKeyChecking=no','-i', keyFile, source, '{0}@{1}:{2}'.format(userName, hostName, clusterParent))
            
        print 'copied cluster control scripts and cluster definition to {0} on {1}'.format(clusterParent, hkey)
        
        scripts = map(os.path.join,[clusterParent,clusterParent],['installjava.py','installgem.py'])
        targets = [javaHome, gemfire]
        for script,target in zip(scripts,targets):
            runRemoteQuietly(keyFile, userName, hostName, 'python', script, target)
            print 'executed {0} {1} on {2}'.format(script, target, hkey)
