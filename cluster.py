#!/usr/bin/python
#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#

from __future__ import print_function
import clusterdef
import json
#import netifaces
import os
import os.path
import re
import socket
import subprocess
import sys
import tempfile


LOCATOR_PID_FILE="cf.gf.locator.pid"
SERVER_PID_FILE="vf.gf.server.pid"

clusterDef = None


def ensureDir(dname):
	if not os.path.isdir(dname):
		os.mkdir(dname)
		
def locatorDir(processName):
	clusterHomeDir = clusterDef.locatorProperty(processName, 'cluster-home')
	return(os.path.join(clusterHomeDir,processName))

def datanodeDir(processName):
	clusterHomeDir = clusterDef.datanodeProperty(processName, 'cluster-home')
	return(os.path.join(clusterHomeDir,processName))


def pidIsAlive(pidfile):
	if not os.path.exists(pidfile):
		return False
		
	with open(pidfile,"r") as f:
		pid = int(f.read())

	proc = subprocess.Popen(["ps",str(pid)], stdin=subprocess.PIPE,
		stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	
	proc.communicate()
	
	if proc.returncode == 0:
		return True
	else:
		return False
	
def serverIsRunning(processName):
	try:
		port = clusterDef.locatorProperty(processName, 'server-port')
		bindAddress = clusterDef.translateBindAddress(clusterDef.datanodeProperty(processName, 'server-bind-address'))
		
		#leave the double parens in the line below!
		sock = socket.create_connection((bindAddress, port))
		sock.close()
		
		return True
	except Exception as x:
		pass
		# ok - probably not running
		
	# now check the pid file
	pidfile = os.path.join(clusterDef.datanodeProperty(processName, 'cluster-home'), processName, SERVER_PID_FILE)
	result = pidIsAlive(pidfile)	
	return result
	
def locatorIsRunning(processName):
	port = clusterDef.locatorProperty(processName, 'port')
	bindAddress = clusterDef.translateBindAddress(clusterDef.locatorProperty(processName, 'bind-address'))
	try:
		#leave the double parens in the line below!
		sock = socket.create_connection( (bindAddress, port))
		sock.close()
		return True
	except Exception as x:
		pass
		# ok - probably not running
		
	# now check the pid file
	pidfile = os.path.join(clusterDef.locatorProperty(processName, 'cluster-home'), processName, LOCATOR_PID_FILE)
	
	return pidIsAlive(pidfile)	
		
def stopLocator(processName):
	GEMFIRE = clusterDef.locatorProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.locatorProperty(processName,'java-home')
	
	if not locatorIsRunning(processName):
		print('{0} is not running'.format(processName))
		return
	try:	
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "stop", "locator"
			,"--dir=" + locatorDir(processName)])
		print('stopped ' + processName)
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)

def stopServer(processName):
	GEMFIRE = clusterDef.datanodeProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.datanodeProperty(processName,'java-home')
	
	if not serverIsRunning(processName):
		print('{0} is not running'.format(processName))
		return
	try:	
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "stop", "server"
			,"--dir=" + datanodeDir(processName)])
		print('stopped ' + processName)
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)


def statusLocator(processName):
	GEMFIRE = clusterDef.locatorProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.locatorProperty(processName,'java-home')
	
	try:
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "status", "locator"
			,"--dir=" + locatorDir(processName)])
		
	except subprocess.CalledProcessError as x:
		sys.exit(x.output)

def statusServer(processName):
	GEMFIRE = clusterDef.datanodeProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.datanodeProperty(processName,'java-home')
	
	try:
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "status", "server"
			,"--dir=" + datanodeDir(processName)])
		
	except subprocess.CalledProcessError as x:
		sys.exit(x.output)

		
def startLocator(processName):
	GEMFIRE = clusterDef.locatorProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.locatorProperty(processName,'java-home')
	
	ensureDir(clusterDef.locatorProperty(processName, 'cluster-home'))
	ensureDir(locatorDir(processName))

	if locatorIsRunning(processName):
		print('locator {0} is already running'.format(processName))
		return
	
	cmdLine = [os.path.join(GEMFIRE,'bin','gfsh')
		, "start", "locator"
		,"--dir=" + locatorDir(processName)
		,"--port={0}".format(clusterDef.locatorProperty(processName, 'port'))
		,'--bind-address={0}'.format(clusterDef.locatorProperty(processName,'bind-address'))
		,"--name={0}".format(processName)]
	
	#these are optional
	if clusterDef.hasLocatorProperty(processName,'hostname-for-clients'):
		cmdLine.append('--hostname-for-clients={0}'.format(clusterDef.locatorProperty(processName, 'hostname-for-clients')))
	
	if clusterDef.hasLocatorProperty(processName,'classpath'):
		cmdLine.append('--classpath={0}'.format(clusterDef.locatorProperty(processName, 'classpath')))
	
	cmdLine[len(cmdLine):] = clusterDef.gfshArgs('locator',processName)
	
	try:
		subprocess.check_call(cmdLine)
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)

def startServerCommandLine(processName):
	GEMFIRE = clusterDef.datanodeProperty(processName,'gemfire')
	
	#the properties in this list are required
	cmdLine = [os.path.join(GEMFIRE,'bin','gfsh')
		, "start", "server"
		,"--dir=" + datanodeDir(processName)
		,"--name={0}".format(processName)
		,"--server-bind-address={0}".format(clusterDef.datanodeProperty(processName,'server-bind-address'))
		,"--server-port={0}".format(clusterDef.datanodeProperty(processName,'server-port'))
		]
	
	#these are optional
	if clusterDef.hasDatanodeProperty(processName,'classpath'):
		cmdLine.append('--classpath={0}'.format(clusterDef.datanodeProperty(processName, 'classpath')))
		
	if clusterDef.hasDatanodeProperty(processName,'spring-xml-location'):
		cmdLine.append('--spring-xml-location={0}'.format(clusterDef.datanodeProperty(processName,'spring-xml-location')))
	
	#all the rest are passed through as -Ds. Those recognized as gemfire properties
	#are prefixed with "gemfire."
	cmdLine[len(cmdLine):] = clusterDef.gfshArgs('datanode',processName)
	
	return cmdLine

# returns a Popen object
def launchServerProcess(processName):
	GEMFIRE = clusterDef.datanodeProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.datanodeProperty(processName,'java-home')
	os.environ['JAVA_ARGS'] = '-Dgfsh.log-dir=. -Dgfsh.log-level=fine'
	#os.environ['JAVA_ARGS'] = '-Dgfsh.log-dir=.'
	
	ensureDir(clusterDef.datanodeProperty(processName, 'cluster-home'))
	ensureDir(datanodeDir(processName))
	
	if serverIsRunning(processName):
		print('{0} is already running'.format(processName))
		return
	
	cmdLine = startServerCommandLine(processName)
	
	try:
		proc = subprocess.Popen(cmdLine)
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)
		
	return proc


def startServer(processName):
	proc = launchServerProcess(processName)

	#could be none if the server was really already running	
	if proc is not None:
		if proc.wait() != 0:
			sys.exit("cache server process failed to start - see the logs in {0}".format(datanodeDir(processName)))


def startClusterLocal():
	
	# probably is only going to be one
	for locator in clusterDef.locatorsOnThisHost():
		startLocator(locator)
		
	procList = []
	for dnode in clusterDef.datanodesOnThisHost():
		proc = launchServerProcess(dnode)
		#can be None if server was already started
		if proc is not None:
			procList.append(proc)

	failCount = 0
	for proc in procList:
		if proc.wait() != 0:
			failCount += 1
			
	if failCount > 0:
		print('at least one server failed to start. Please check the logs for more detail')

def stopClusterLocal():
	
	for dnode in clusterDef.datanodesOnThisHost():
		stopServer(dnode)

	# probably is only going to be one
	for locator in clusterDef.locatorsOnThisHost():
		stopLocator(locator)
		
			
def stopCluster():
	GEMFIRE = None
	JAVA = None
	processList = clusterDef.locatorsOnThisHost()
	if len(processList) > 0:
		GEMFIRE = clusterDef.locatorProperty(processList[0],'gemfire')
		JAVA_HOME = clusterDef.locatorProperty(processList[0],'java-home')
	else:
		processList = clusterDef.datanodesOnThisHost()
		if len(processList) > 0:
			GEMFIRE = clusterDef.locatorProperty(processList[0],'gemfire')
			JAVA_HOME = clusterDef.locatorProperty(processList[0],'java-home')
		else:
			sys.exit('no cluster processes are on this host - unable to ascertain gfsh setup information')
			
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = JAVA_HOME

	# pick any locator and connect to it
	success = False
	for hkey in clusterDef.clusterDef['hosts']:
		host = clusterDef.clusterDef['hosts'][hkey]
		for pkey in host['processes']:
			process = host['processes'][pkey]
			if process['type'] == 'locator':
				if not success:
					bindAddress = clusterDef.locatorProperty(pkey,'bind-address', host = hkey)
					port = clusterDef.locatorProperty(pkey,'port', host = hkey)
					GEMFIRE = clusterDef.locatorProperty(pkey,'gemfire', host = hkey)
					rc = subprocess.call([GEMFIRE + "/bin/gfsh"
						, "-e", "connect --locator={0}[{1}]".format(bindAddress,port)
						,"-e", "shutdown"])
					if rc == 0:
						success = True
			
	if success == False:
		sys.exit('could not shut down cluster')
					
	
def printUsage():
	print('Usage:')
	print('   cluster.py  [--cluster-def=path/to/clusterdef.json] start <process-name>')
	print('   cluster.py  [--cluster-def=path/to/clusterdef.json] stop <process-name>')
	print('   cluster.py  [--cluster-def=path/to/clusterdef.json] status <process-name>')
	print()
	print('   cluster.py [--cluster-def=path/to/clusterdef.json] start')
	print('   cluster.py [--cluster-def=path/to/clusterdef.json] stop')
	print('Notes:')
	print('* all commands are idempotent')
	
	
def subEnvVars(aString):
	result = aString
	varPattern = re.compile(r'\${(.*)}')
	match = varPattern.search(result)
	while match is not None:
		envVarName = match.group(1)
		if envVarName in os.environ:
			result = result.replace(match.group(0), os.environ[envVarName])
		
		match = varPattern.search(result, match.end(0) + 1)
		
	return result

if __name__ == '__main__':
	if len(sys.argv) == 1:
		printUsage()
		sys.exit(0)

	nextIndex = 1
	clusterDefFile = None
	
	#now process -- args
	while nextIndex < len(sys.argv):
		if sys.argv[nextIndex].startswith('--'):
			if sys.argv[nextIndex].startswith('--cluster-def='):
				clusterDefFile = sys.argv[nextIndex][len('--cluster-def='):]
			else:
				sys.exit('{0} is not a recognized option'.format(sys.argv[nextIndex]))
			nextIndex += 1
		else:
			break
		
	if clusterDefFile is None:
		here = os.path.dirname(sys.argv[0])
		clusterDefFile = os.path.join(here, 'cluster.json')
		
	if not os.path.isfile(clusterDefFile):
		sys.exit('could not find cluster definition file: ' + clusterDefFile)
		
	# copy the whole file to a temp file line by line doing env var
	# substitutions along the way, then load the cluster defintion
	# from the temp file
	with open(clusterDefFile,'r') as f:
		tfile = tempfile.NamedTemporaryFile(delete=False)
		tfileName = tfile.name
		with  tfile:
			line = f.readline()
			while(len(line) > 0):
				tfile.write(subEnvVars(line))
				line = f.readline()
				
	with open(tfileName,'r') as f:
		clusterDef = clusterdef.ClusterDef(json.load(f))
	
	os.remove(tfileName)
		
	if nextIndex >= len(sys.argv):
		sys.exit('invalid input, please provide a command')
		
	cmd = sys.argv[nextIndex]
	nextIndex += 1
	
	if len(sys.argv) == nextIndex:
		if cmd == 'start':
			startClusterLocal()
		elif cmd == 'stop':
			stopCluster()
		else:
			sys.exit('unknown command: ' + cmd)
	else:
		obj = sys.argv[nextIndex]
		nextIndex += 1
		
		if clusterDef.isLocatorOnThisHost(obj):
			if cmd == 'start':
				startLocator(obj)
			elif cmd == 'stop':
				stopLocator(obj)
			elif cmd == 'status':
				statusLocator(obj)
			else:
				sys.exit(cmd + ' is an unkown operation for locators')
				
		elif clusterDef.isDatanodeOnThisHost(obj):
			if cmd == 'start':
				startServer(obj)
			elif cmd == 'stop':
				stopServer(obj)
			elif cmd == 'status':
				statusServer(obj)
			else:
				sys.exit(cmd + ' is an unkown operation for datanodes')		
			
		else:
			sys.exit(obj + ' is not defined for this host or is not a known process type')		
	

		
