import os.path
import subprocess
import sys
import urllib

if __name__ == '__main__':
    if not os.path.exists('pivotal-gemfire-9.0.4.zip'):
        print 'retrieving gemfire - stand by ...'
        urllib.urlretrieve('http://download.pivotal.com.s3.amazonaws.com/gemfire/9.0.4/pivotal-gemfire-9.0.4.zip','pivotal-gemfire-9.0.4.zip')

    print 'pivotal-gemfire-9.0.4.zip downloaded'

    cmd = 'keytool -genkey -alias self ' + \
        '-keystore trusted.keystore -storepass passw0rd ' + \
        '-storetype JKS ' + \
        '-keypass passw0rd ' + \
        '-dname CN=trusted ' + \
        '-validity 3650'

    print cmd
    rc = subprocess.call(cmd.split())
    if rc == 0:
        print 'keystore generated'
    else:
        sys.exit('keystore generation failed')
