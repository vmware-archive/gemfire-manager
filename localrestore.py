import subprocess
import os
import os.path
import sys

if __name__ == '__main__':
   backupDir = sys.argv[1]
   if not os.path.isdir(backupDir):
      sys.exit('{0} is not a directory, exiting'.format(backupDir))
      
   for subdir in os.listdir(backupDir):
      restoreScript = os.path.join(backupDir,subdir,'restore.sh')
      if not os.path.exists(restoreScript):
         sys.exit('restore scripts could not be found in ' + backupDir)
         
      subprocess.check_call([restoreScript])
      print '{0} executed successfully'.format(restoreScript)
      