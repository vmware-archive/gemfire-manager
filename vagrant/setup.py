import os.path
import subprocess
import sys
import urllib

if __name__ == '__main__':
    gemfire_version = '9.3.0'
    gemfire_filename = 'pivotal-gemfire-{0}.zip'.format(gemfire_version)

    if not os.path.exists(gemfire_filename):
        print 'retrieving gemfire - stand by ...'
        urllib.urlretrieve('http://download.pivotal.com.s3.amazonaws.com/gemfire/{0}/{1}'.format(gemfire_version,gemfire_filename),gemfire_filename)
        print '{0} downloaded'.format(gemfire_filename)
    else:
        print '{0} is already downloaded'.format(gemfire_filename)

    keystore_file = 'trusted.keystore'
    keytool_command = 'keytool -genkey -alias self -dname CN=trusted -validity 3650 -keypass password -keystore {0} -storepass password -storetype JKS'.format(keystore_file)
    if os.path.exists(keystore_file):
        print '{0} already exists'.format(keystore_file)
    else:
        subprocess.check_call(keytool_command.split())
        print 'created key file: {0}'.format(keystore_file)
