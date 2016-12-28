import json
import os.path
import subprocess
import sys

#TODO check to see if the cluster is running




def runRestore(ssh, backupDir):
   here = os.path.dirname(os.path.abspath(sys.argv[0]))
   sshOptions = [ '-o','UserKnownHostsFile=/dev/null','-o', 'StrictHostKeyChecking=no','-q', '-i', ssh['key-file']]
   source = os.path.join(here, 'localrestore.py')
   target = '{0}@{1}:/tmp'.format(ssh['user'],ssh['host'])
   cmd = ['scp'] + sshOptions + [source,target]
   process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
   process.communicate()
   if process.returncode != 0:
      sys.exit('failure uploading  script: ' + process.stdout)

   cmd = ['ssh'] + sshOptions + ['{0}@{1}'.format(ssh['user'],ssh['host']),'/usr/bin/python','/tmp/localrestore.py', backupDir]
   subprocess.check_call(cmd)
   print 'restored {0} on {1}'.format(backupDir, ssh['host'])

if __name__ == '__main__':
   if len(sys.argv) != 2:
      sys.exit('please supply the full path to the backup you wish to restore on the remote cluster')
      
   backupDir = sys.argv[1]
   
   here = os.path.dirname(os.path.abspath(sys.argv[0]))
   with open(os.path.join(here,'InstallGemFireCluster','cluster.json'),'r') as f:
      config = json.load(f)
      
      
   for serverName in config['hosts']:
      if serverName == 'dummy':
         continue
      
      server= config['hosts'][serverName]
      runRestore(server['ssh'], backupDir)
