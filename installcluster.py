#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#
import clusterdef
import json
import os
import os.path
import subprocess
import sys

#
# HELPER FUNCTIONS
#
def runListQuietly(args):
    """ args shold be a list """
    p = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = p.communicate()
    if p.returncode != 0:
        sys.exit('command failed: ' + ' '.join(list(args)))

def runQuietly(*args):
    runListQuietly(list(args))

def runRemote(sshKeyPath, user, host, *args):
    prefix = ['ssh', '-o','StrictHostKeyChecking=no',
                     '-o', 'UserKnownHostsFile=/dev/null',
                        '-t',
                        '-q',
                        '-i', sshKeyPath,
                        '{0}@{1}'.format(user, host)]

    try:
        subprocess.check_call(prefix + list(args))
    except subprocess.CalledProcessError as e:
        sys.exit('remote command as {0} on {1} failed: '.format(user,host) + ' '.join(list(args)))

#
# CONSTANTS
#
GEMTOOLS_ARCHIVE = 'gemfire-toolkit-N-runtime.tar.gz'

#
# MAIN
#
if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('please provide the path to the cluster definition file as the first argument')

    here = os.path.dirname(sys.argv[0])
    if len(here) == 0:
        here = '.'

    #read cluster.json
    clusterDefFile = sys.argv[1]

    with open(clusterDefFile, 'r') as contextFile:
        clusterDefRaw = json.load(contextFile)

    clusterDef = clusterdef.ClusterDef(clusterDefFile)

    # TODO shouldn't be necessary to have both clusterDef and clusterDefRaw

    # build gemfire toolkit
    p = subprocess.Popen(['mvn','package'],cwd='gemfire-toolkit', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.communicate()
    if p.returncode != 0:
        sys.exit('maven build of gemfire toolkit failed')

    print 'gemfire toolkit built'


    for hkey in clusterDefRaw['hosts']:
        host = clusterDefRaw['hosts'][hkey]
        # pick the first process and use it to look up the gemfire and java-home settings
        gemfire = None
        javaHome = None
        clusterHome = None
        for pkey in host['processes']:
            process = host['processes'][pkey]
            clusterHome = clusterDef.processProperty(process['type'],pkey,'cluster-home', host=hkey)
            if clusterHome is not None:
                break

        if clusterHome is None:
            sys.exit('could not look up cluster-home settings for host {0}'.format(hkey))

        #copy the scripts and cluster.json to the cluster-home directory
        keyFile = host['ssh']['key-file']
        hostName = host['ssh']['host']
        userName = host['ssh']['user']
        if not os.path.isfile(keyFile):
            sys.exit('key file {0} not found'.format(keyFile))

        os.chmod(keyFile,0600)

        # create the clusterParent dir on the remote host if it does not exist
        runQuietly('scp','-o','StrictHostKeyChecking=no','-o','UserKnownHostsFile=/dev/null','-i', keyFile, os.path.join(here, 'ensuredirs.py'), '{0}@{1}:/tmp'.format(userName, hostName))
        runRemote(keyFile, userName, hostName, 'python', '/tmp/ensuredirs.py', clusterHome)

        sources = ['cluster.py', 'clusterdef.py','gemprops.py','gf.py',clusterDefFile]
        for s in sources:
            source = os.path.join(here, s)
            runQuietly('scp','-o','StrictHostKeyChecking=no','-o','UserKnownHostsFile=/dev/null','-i', keyFile, source, '{0}@{1}:{2}'.format(userName, hostName, clusterHome))

        print 'copied cluster control scripts and cluster definition to {0} on {1}'.format(clusterHome, hkey)

        source = os.path.join('gemfire-toolkit','target',GEMTOOLS_ARCHIVE)
        runQuietly('scp','-o','StrictHostKeyChecking=no','-o','UserKnownHostsFile=/dev/null','-i', keyFile, source, '{0}@{1}:{2}'.format(userName, hostName, clusterHome))
        runRemote(keyFile, userName,hostName, 'tar', '-C', clusterHome, '-xzf', os.path.join(clusterHome,GEMTOOLS_ARCHIVE))
        print 'copied and unpacked gemfire toolkit on {0}'.format(hkey)
