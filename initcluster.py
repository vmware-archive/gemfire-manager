#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#
import clusterdef
import json
import os
import os.path
import platform
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
    here = os.path.dirname(os.path.abspath(sys.argv[0]))

    if platform.system() == 'Windows':
        gfsh_cmd = 'gfsh.bat'
    else:
        gfsh_cmd = 'gfsh'

    dataDir = 'data/pdx'

    clusterDefFile = os.path.join(here, 'cluster.json')
    cluster = clusterdef.ClusterDef(clusterDefFile)
    locators = cluster.locatorProperty('locator1','locators')

    GEMFIRE = cluster.locatorProperty('locator1','gemfire')
    os.environ['GEMFIRE'] = GEMFIRE
    os.environ['JAVA_HOME'] = cluster.locatorProperty('locator1','java-home')

    gfsh = os.path.join(GEMFIRE,'bin',gfsh_cmd)

    connect_cmd = [gfsh,'-e','connect --locator={0}'.format(locators)]
    create_diskstore_cmd = ['-e','create disk-store --name=pdx-disk-store --dir={0}'.format(dataDir)]
    configpdx_cmd = ['-e','configure pdx --disk-store=pdx-disk-store --read-serialized=true']
    #shutdown_cmd = ['-e','shutdown --include-locators=true']

    # make sure the cluster is up
    subprocess.check_call(['python',os.path.join(here,'cluster.py'), 'start'], cwd = here)

    # configure pdx and bring the cluster down
    subprocess.check_call(connect_cmd + create_diskstore_cmd + configpdx_cmd , cwd = here)

    # make sure the cluster is down
    subprocess.check_call(['python',os.path.join(here,'cluster.py'), 'stop'], cwd = here)

    print('cluster initialized, use "python cluster.py start" to start')
