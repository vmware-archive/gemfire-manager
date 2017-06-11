import json
import os.path
import subprocess
import sys

#TODO check to see if the cluster is running




def remoteWipe(ssh, dirname):
   here = os.path.dirname(os.path.abspath(sys.argv[0]))
   sshOptions = [ '-o','UserKnownHostsFile=/dev/null','-o', 'StrictHostKeyChecking=no','-q', '-i', ssh['key-file']]
   source = os.path.join(here, 'wipelocaldiskstore.py')
   target = '{0}@{1}:/tmp'.format(ssh['user'],ssh['host'])
   cmd = ['scp'] + sshOptions + [source,target]
   process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
   process.communicate()
   if process.returncode != 0:
      sys.exit('failure uploading  script: ' + process.stdout)

   cmd = ['ssh'] + sshOptions + ['{0}@{1}'.format(ssh['user'],ssh['host']),'/usr/bin/python','/tmp/wipelocaldiskstore.py', dirname]
   process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
   outstreams = process.communicate()
   if process.returncode != 0:
      sys.exit('failure running remote script: ' + outstreams[0])
      
   print 'wiped {0} on {1}'.format(dirname, ssh['host'])

if __name__ == '__main__':
   answer = raw_input('This will wipe out all of the data in the whole cluster - are you sure ? (Y/N)')
   if answer != 'Y':
      sys.exit('Exiting.')
   
   here = os.path.dirname(os.path.abspath(sys.argv[0]))
   with open(os.path.join(here,'InstallGemFireCluster','cluster.json'),'r') as f:
      config = json.load(f)
      
   clusterHome = config['global-properties']['cluster-home']
      
   for serverName in config['hosts']:
      if serverName == 'dummy':
         continue
      
      locators = []
      server= config['hosts'][serverName]
      for processName in server['processes']:
         process = server['processes'][processName]
         if process['type'] == 'locator':
            locators.append(processName)
      
      for locator in locators:
         diskStoreDir = os.path.join(clusterHome,locator,'ConfigDiskDir_{0}'.format(locator))
         remoteWipe(server['ssh'], diskStoreDir)
         
      for diskStoreDir in ['/data/person','/data/pdx']:
         remoteWipe(server['ssh'], diskStoreDir)
         
