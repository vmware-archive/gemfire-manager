#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#
import os
import os.path
import sys

if __name__ == '__main__':
    if len(sys.argv) == 1:
        sys.exit('no arguments provided. please provide one or more directory names')
        
    for dname in sys.argv[1:]:
        if os.path.exists(dname):
            if os.path.isfile(dname):
                sys.exit('cannot create directory {0} because a file with the same name already exists'.format(dname))
        else:
            os.makedirs(dname)
            print 'created directory {0}'.format(dname)
        
    