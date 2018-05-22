import os
import os.path
import subprocess
import sys

gfsh='/usr/local/Cellar/gemfire/9.3.0/bin/gfsh'

def listkey(scriptname):
    dash = scriptname.find('-')
    if dash < 0:
        return 0
    else:
        return int(scriptname[0:dash])

if __name__ == '__main__':
    here = os.path.dirname(os.path.abspath(sys.argv[0]))
    gf = os.path.join(here,'gf.py')
    clusterdef = os.path.join(here,'vagrant','cluster.json')
    
    script_dir = os.path.join(here,'gemfire-scripts')
    for filename in sorted(os.listdir(script_dir),key=listkey):
        subprocess.check_call([gfsh,'-e','connect --locator=10.10.10.101[10000]','-e','run --file={0}'.format(os.path.join(script_dir,filename))])
        if filename.find('restart') > 0:
            subprocess.check_call(['python',gf,clusterdef,'bounce'])
        
