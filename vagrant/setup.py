import os.path
import subprocess
import sys
import urllib

if __name__ == '__main__':
    gemfire_version = '9.2.2'
    gemfire_filename = 'pivotal-gemfire-{0}.zip'.format(gemfire_version)

    if not os.path.exists(gemfire_filename):
        print 'retrieving gemfire - stand by ...'
        urllib.urlretrieve('http://download.pivotal.com.s3.amazonaws.com/gemfire/{0}/{1}'.format(gemfire_version,gemfire_filename),gemfire_filename)
        print '{0} downloaded'.format(gemfire_filename)
    else:
        print '{0} is already downloaded'.format(gemfire_filename)
