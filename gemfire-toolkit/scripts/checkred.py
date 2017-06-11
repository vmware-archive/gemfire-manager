#!/usr/bin/python
import os
import os.path
import subprocess
import sys

if  not os.environ.has_key("JAVA_HOME"):
    sys.exit("JAVA_HOME environment variable must be configured")

here = os.path.dirname(sys.argv[0])

classpath = os.path.join(here,"lib","*")
java = os.path.join(os.environ["JAVA_HOME"],"bin","java")

args = [java, "-cp", classpath, "io.pivotal.gemfire.extensions.tools.CheckRedundancy"]
args = args + sys.argv[1:]
os.execv(java, args)
