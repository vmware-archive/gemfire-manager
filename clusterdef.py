#
# Copyright (c) 2015-2016 Pivotal Software, Inc. All Rights Reserved.
#
import gemprops
import json
import os
import re
import socket
import subprocess
import tempfile



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


class ClusterDef:

    def __init__(self, cluster_def_filename):
        # copy the whole file to a temp file line by line doing env var
        # substitutions along the way, then load the cluster defintion
        # from the temp file
        with open(cluster_def_filename,'r') as f:
            tfile = tempfile.NamedTemporaryFile(mode='wt',delete=False)
            tfileName = tfile.name
            with  tfile:
                line = f.readline()
                while(len(line) > 0):
                    tfile.write(subEnvVars(line))
                    line = f.readline()

        with open(tfileName,'r') as f:
            self.clusterDef = json.load(f)

        os.remove(tfileName)

        self.thisHost = socket.gethostname()



    @staticmethod
    def determineExternalHost(ipaddress):

             #Determine ip address
            process = subprocess.Popen(["nslookup", ipaddress], stdout=subprocess.PIPE)
            output = str(process.communicate()[0])
            startEc2 = output.find("name = ec2-")
            startEc2 = startEc2+7
            endEc2 = output.find("amazonaws.com",startEc2)+13

            externalHost = output[startEc2:endEc2]
            return externalHost

    def hostName(self):
        return self.thisHost

    def isBindAddressProperty(self, propName):
        if propName.endswith('bind-address'):
            return True

        if propName.endswith('BIND_ADDRESS'):
            return True

        return False


    # if addr does not contain a "." it will be treated as a network interface
    # name and translated into an ip address using the netifaces package
    def translateBindAddress(self,addr):
        if not '.' in addr:
            import netifaces
            if addr in netifaces.interfaces():
                #TODO does this ever return an ipV6 address ?  Is that a problem ?
                return netifaces.ifaddresses(addr)[netifaces.AF_INET][0]['addr']
            else:
                # in this case, assume it is a host name
                return addr
        else:
            return addr


    def isProcessOnThisHost(self, processName, processType):
        result = False
        for hostname in [self.thisHost,'localhost']:
            if hostname in self.clusterDef['hosts']:
                if processName in self.clusterDef['hosts'][hostname]['processes']:
                    process = self.clusterDef['hosts'][hostname]['processes'][processName]
                    result =  process['type'] == processType
                    break

        return result


    # raises an exception if a process with the given name is not defined for
    # this host
    def processProps(self, processName, host=None):
        processes = None
        if host is None:
            thishost = self.thisHost
        else:
            thishost = host

        if thishost in self.clusterDef['hosts']:
            processes = self.clusterDef['hosts'][thishost]['processes']

        elif host is None and 'localhost' in self.clusterDef['hosts']:
            processes = self.clusterDef['hosts']['localhost']['processes']

        else:
            raise Exception('this host ({0}) not found in cluster definition'.format(thishost))

        return processes[processName]


    #host props are optional - if they are not defined in the file an empty
    #dictionary will be returned
    def hostProps(self, host = None):
        result = dict()

        if host is None:
            thishost = self.thisHost
        else:
            thishost = host

        if thishost in self.clusterDef['hosts']:
            result = self.clusterDef['hosts'][thishost]['host-properties']

        elif host is None and 'localhost'  in self.clusterDef['hosts']:
            result = self.clusterDef['hosts']['localhost']['host-properties']

        return result

    #scope can be locator-properties, datanode-properties or global-properties
    #all are optional
    def props(self, scope):
        if scope in self.clusterDef:
            return self.clusterDef[scope]
        else:
            return dict()

    # this is the main method for accessing properties
    def processProperty(self, processType, processName, propertyName, host = None, notFoundOK=False):
        pProps = self.processProps(processName, host = host)
        if propertyName in pProps:
            return pProps[propertyName]

        hostProps = self.hostProps(host = host)
        if propertyName in hostProps:
            return hostProps[propertyName]

        locProps = self.props(processType + '-properties')
        if propertyName in locProps:
            return locProps[propertyName]

        globProps = self.props('global-properties')
        if propertyName in globProps:
            return globProps[propertyName]
        else:
            if notFoundOK:
                return None
            else:
                raise Exception('property not found: ' + propertyName)


    # this method assumes that it is not passed handled props or
    # jvm props
    def gfshArg(self, key, val):
        if self.isBindAddressProperty(key):
            val = self.translateBindAddress(val)

        if key in gemprops.GEMFIRE_PROPS or key.startswith('security-'):
            return '--J="-Dgemfire.{0}={1}"'.format(key,val)

        else:
            return '--J="-D{0}={1}"'.format(key,val)


    def buildGfshArgs(self, props):
        result = []
        for key in list(props.keys()):
            if not key in gemprops.HANDLED_PROPS:
                result.append(self.gfshArg(key, props[key]))

        return result

    def processesOnThisHost(self, processType):
        result = []
        for hostname in [self.thisHost, 'localhost']:
            if hostname in self.clusterDef['hosts']:
                for processName in list(self.clusterDef['hosts'][hostname]['processes'].keys()):
                    process = self.clusterDef['hosts'][hostname]['processes'][processName]
                    if process['type'] == processType:
                        result.append(processName)

        return result

# public interface

    def locatorsOnThisHost(self):
        return self.processesOnThisHost('locator')


    def datanodesOnThisHost(self):
        return self.processesOnThisHost('datanode')

    def accessorsOnThisHost(self):
        return self.processesOnThisHost('accessor')


    def isLocatorOnThisHost(self, processName):
        return self.isProcessOnThisHost(processName, 'locator')


    def isDatanodeOnThisHost(self, processName):
        return self.isProcessOnThisHost(processName, 'datanode')


    #TODO it should not be necessary to pass the host in any case becaus the
    #     processName implies the host
    def locatorProperty(self, processName, propertyName, host=None, notFoundOK = False):
        result = self.processProperty('locator',processName, propertyName, host = host, notFoundOK = notFoundOK)
        if result is None:
            return result

        if self.isBindAddressProperty(propertyName):
            return self.translateBindAddress(result)
        else:
            return result


    #TODO it should not be necessary to pass the host in any case becaus the
    #     processName implies the host
    def datanodeProperty(self, processName, propertyName, host=None, notFoundOK = False):
        result = self.processProperty('datanode',processName, propertyName, host = host, notFoundOK = notFoundOK)
        if self.isBindAddressProperty(propertyName):
            return self.translateBindAddress(result)
        else:
            return result

    #TODO it should not be necessary to pass the host in any case becaus the
    #     processName implies the host
    def hasDatanodeProperty(self, processName, propertyName, host = None):
        pProps = self.processProps(processName, host = host)
        if propertyName in pProps:
            return True

        hostProps = self.hostProps(host = host)
        if propertyName in hostProps:
            return True

        dnProps = self.props('datanode-properties')
        if propertyName in dnProps:
            return True

        globProps = self.props('global-properties')
        if propertyName in globProps:
            return True
        else:
            return False

    #TODO - extract bits common to this and hasDatanodeProperty and
    # put in shared function
    #TODO it should not be necessary to pass the host in any case becaus the
    #     processName implies the host
    def hasLocatorProperty(self, processName, propertyName, host = None):
        pProps = self.processProps(processName, host = host)
        if propertyName in pProps:
            return True

        hostProps = self.hostProps(host = host)
        if propertyName in hostProps:
            return True

        lProps = self.props('locator-properties')
        if propertyName in lProps:
            return True

        globProps = self.props('global-properties')
        if propertyName in globProps:
            return True
        else:
            return False



    def gfshArgs(self, processType, processName):
        temp = dict()
        #note that order is important here - process specific properties
        #override host properties override datanode/locator properties
        #override global properties
        for source in [self.props('global-properties'),
                       self.props(processType + '-properties'),
                       self.hostProps(),
                       self.processProps(processName)]:
            for k in list(source.keys()):
                temp[k] = source[k]

        #now post-process, removing the items that cannot be passed as
        #-Ds and prefixing the remainders
        result = self.buildGfshArgs(temp)

        # now directly add the contents of jvm-options' if present
        if 'jvm-options' in temp:
            for option in temp['jvm-options']:
                result.append('--J={0}'.format(option))

        return result
