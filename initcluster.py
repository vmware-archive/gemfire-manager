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
    if 'GEMFIRE' not in os.environ:
        sys.exit('Please set the GEMFIRE environment variable')

    here = os.path.dirname(os.path.abspath(sys.argv[0]))

    dataDir = 'data/pdx'

    clusterDefFile = os.path.join(here, 'cluster.json')
    with open(clusterDefFile,'r') as cdfile:
        cdef = json.load(cdfile)

    cluster = clusterdef.ClusterDef(cdef)
    locators = cluster.locatorProperty('locator1','locators')

    gfsh = os.path.join(os.environ['GEMFIRE'],'bin','gfsh')

    connect_cmd = [gfsh,'-e','connect --locator={0}'.format(locators)]
    create_diskstore_cmd = ['-e','create disk-store --name=pdx-disk-store --dir={0}'.format(dataDir)]
    configpdx_cmd = ['-e','configure pdx --disk-store=pdx-disk-store --read-serialized=true']
    shutdown_cmd = ['-e','shutdown --include-locators=true']

    # make sure the cluster is up
    subprocess.check_call(['python',os.path.join(here,'cluster.py'), 'start'], cwd = here)

    # configure pdx and bring the cluster down
    subprocess.check_call(connect_cmd + create_diskstore_cmd + configpdx_cmd + shutdown_cmd, cwd = here)

    # start the cluster again
    subprocess.check_call(['python',os.path.join(here,'cluster.py'), 'start'], cwd = here)
