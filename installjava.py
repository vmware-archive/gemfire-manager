#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#
import os.path
import shutil
import subprocess
import sys

# this script can be run on a host to install gemfire and java

# these hard coded keys have (only) read access to the software buckets
AWS_ACCESS_KEY_ID = 'AKIAJXWLAUH63ULBFOPA'
AWS_SECRET_ACCESS_KEY = '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'
AWS_S3_BUCKET_REGION = 'us-west-2'
AWS_S3_BUCKET = 's3://rmay.pivotal.io.software/'
JDK_FILE = 'jdk-8u92-linux-x64.tar.gz'
JDK='jdk1.8.0_92' # this is the name of the root directory in the java tar

def runQuietly(*args):
    p = subprocess.Popen(list(args), stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = p.communicate()
    if p.returncode != 0:
        raise Exception('"{0}" failed with the following output: {1}'.format(' '.join(list(args)), output[0]))

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit('please provide the install location and no other arguments')
        
    target = sys.argv[1]
    targetDir = os.path.dirname(target)
    if not os.path.isdir(targetDir):
        sys.exit('target directory {0} does not exist'.format(targetDir))
    
    runQuietly('aws', 'configure', 'set', 'aws_access_key_id', AWS_ACCESS_KEY_ID)
    runQuietly('aws', 'configure', 'set', 'aws_secret_access_key', AWS_SECRET_ACCESS_KEY)
    runQuietly('aws', 'configure', 'set', 'default.region', AWS_S3_BUCKET_REGION)
    
    runQuietly('aws', 's3', 'cp', AWS_S3_BUCKET + JDK_FILE, '/tmp')
    runQuietly('tar', '-C', targetDir, '-xzf', '/tmp/' + JDK_FILE)
    runQuietly('ln', '-s', os.path.join(targetDir,JDK), target)
    print 'downloaded and installed java'
        