#!/usr/bin/python

from __future__ import print_function
import clusterdef
import json
#import netifaces
import os
import os.path
import socket
import subprocess
import sys


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
	
	if proc.wait() != 0:
		sys.exit("cache server process failed to start - see the logs in {0}".format(datanodeDir(processName)))

	
def startClusterLocal():
	
	# probably is only going to be one
	for locator in clusterDef.locatorsOnThisHost():
		startLocator(locator)
		
	procList = []
	for dnode in clusterDef.datanodesOnThisHost():
		procList.append(launchServerProcess(dnode))

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
		
			
def stopCluster(cnum):
	if not locatorIsRunning():
		return
		
	rc = subprocess.call([GEMFIRE + "/bin/gfsh"
		, "-e", "connect --locator=localhost[{0}]".format(locatorport(cnum))
		,"-e", "shutdown"])

	# it appears that the return code in this case is not correct
	# will just hope for the best right now	
	
	stopLocator(cnum)
	
def printUsage():
	print('Usage:')
	print('   cluster.py <path-to-cluster-def> start <process-name>')
	print('   cluster.py <path-to-cluster-def> stop <process-name>')
	print('   cluster.py <path-to-cluster-def> status <process-name>')
	print()
	print('   cluster.py <path-to-cluster-def> start')
	print('   cluster.py <path-to-cluster-def> stop')
	print('Notes:')
	print('* all commands are idempotent')
	
if __name__ == '__main__':
	if len(sys.argv) == 1:
		printUsage()
		sys.exit(0)
		
	clusterDefFile = sys.argv[1]
	if not os.path.isfile(clusterDefFile):
		sys.exit('could not find cluster definition file: ' + clusterDefFile)
		
	with open(clusterDefFile,'r') as f:
		clusterDef = clusterdef.ClusterDef(json.load(f))
		
	if len(sys.argv) < 3:
		sys.exit('invalid input, please provide a command')
		
	cmd = sys.argv[2]
	
	if len(sys.argv) == 3:
		if cmd == 'start':
			startClusterLocal()
		elif cmd == 'stop':
			stopClusterLocal()
		else:
			sys.exit('unknown command: ' + cmd)
	else:
		obj = sys.argv[3]
		
		if clusterDef.isLocator(obj):
			if cmd == 'start':
				startLocator(obj)
			elif cmd == 'stop':
				stopLocator(obj)
			elif cmd == 'status':
				statusLocator(obj)
			else:
				sys.exit(cmd + ' is an unkown operation for locators')
				
		elif clusterDef.isDatanode(obj):
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
	

		
