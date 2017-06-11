import os.path
import urllib

if __name__ == '__main__':
    if not os.path.exists('pivotal-gemfire-9.0.4.zip'):
        print 'retrieving gemfire - stand by ...'
        urllib.urlretrieve('http://download.pivotal.com.s3.amazonaws.com/gemfire/9.0.4/pivotal-gemfire-9.0.4.zip','pivotal-gemfire-9.0.4.zip')

    print 'pivotal-gemfire-9.0.4.zip downloaded'
