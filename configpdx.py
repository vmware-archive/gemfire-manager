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
# MAIN
#
if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit('please provide the path to the cluster definition file as the first argument and the path to the gemfire data directory on the remote servers as the second argument')

    here = os.path.dirname(sys.argv[0])
    if len(here) == 0:
        here = '.'

    #read cluster.json
    clusterDefFile = sys.argv[1]

    remoteDataDir = sys.argv[2]

    print '>>> starting cluster ...'
    subprocess.check_call(['python',os.path.join(here, 'gf.py'), clusterDefFile, 'start'])

    print '>>> configuring PDX'
    subprocess.check_call(['python',os.path.join(here, 'gf.py'), clusterDefFile, 'gfsh','create','disk-store','--name=pdx-disk-store', '--dir=' + os.path.join(remoteDataDir,'pdx')])
    subprocess.check_call(['python',os.path.join(here, 'gf.py'), clusterDefFile, 'gfsh','configure','pdx','--disk-store=pdx-disk-store', '--read-serialized=true'])

    print '>>> stopping cluster'
    subprocess.check_call(['python',os.path.join(here, 'gf.py'), clusterDefFile, 'gfsh','shutdown','--include-locators=true'])
