import gemprops
import netifaces
import socket

#conventions
# anything starting with "gemfire." will be passed to started processes
# as additional options
#

HANDLED_PROPS=['gemfire','java-home','cluster-home', 'bind-address', 'port',
               'jvm-options','server-bind-address', 'server-port', 'classpath']

GEMFIRE_PROPS=['locators','jmx-manager-bind-address','jmx-manager-port',
               'http-service-bind-address','http-service-port',
               'conserve-sockets','cache-xml-file','distributed-system-id', 'remote-locators',
               'statistic-sampling-enabled', 'statistic-archive-file',
               'log-disk-space-limit', 'log-file-size-limit','log-file',
               'archive-disk-space-limit', 'archive-file-size-limit',
	       'tombstone-gc-threshold']

class ClusterDef:
    
    def __init__(self, cdef):
        self.clusterDef = cdef
        self.thisHost = socket.gethostname()

     
    def hostName(self):
        return self.thisHost
        
    def isBindAddressProperty(self, propName):
        return propName.endswith('bind-address')

    
    # if addr does not contain a "." it will be treated as a network interface
    # name and translated into an ip address using the netifaces package
    def translateBindAddress(self,addr):
        if not '.' in addr:
            #TODO does this ever return an ipV6 address ?  Is that a problem ?
            return netifaces.ifaddresses(addr)[netifaces.AF_INET][0]['addr']
        else:
            return addr


    #TODO - maybe it would make more sense for all methods to
    # target "this host" implicitly
    def isProcessOnThisHost(self, processName, processType):
        if self.thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))

        if not processName in self.clusterDef['hosts'][self.thisHost]['processes']:
            return False
        
        process = self.clusterDef['hosts'][self.thisHost]['processes'][processName]
        return process['type'] == processType
        

    # raises an exception if a process with the given name is not defined for
    # this host
    def processProps(self, processName):
        if self.thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))

        if not processName in self.clusterDef['hosts'][self.thisHost]['processes']:
            raise Exception('{0} is not a valid process name on this host ({1})'.format(processName,self.thisHost))
        
        return self.clusterDef['hosts'][self.thisHost]['processes'][processName]

    
    #host props are optional - if they are not defined in the file an empty
    #dictionary will be returned
    def hostProps(self):
        if self.thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))
        
        if 'host-properties' in self.clusterDef['hosts'][self.thisHost]:
            return self.clusterDef['hosts'][self.thisHost]['host-properties']
        else:
            return dict()

        
    #scope can be locator-properties, datanode-properties or global-properties
    #all are optional
    def props(self, scope):
        if scope in self.clusterDef:
            return self.clusterDef[scope]
        else:
            return dict()

        
    def processProperty(self, processType, processName, propertyName):
        pProps = self.processProps(processName)
        if propertyName in pProps:
            return pProps[propertyName]
        
        hostProps = self.hostProps()
        if propertyName in hostProps:
            return hostProps[propertyName]
        
        locProps = self.props(processType + '-properties')
        if propertyName in locProps:
            return locProps[propertyName]
        
        globProps = self.props('global-properties')
        if propertyName in globProps:
            return globProps[propertyName]
        else:
            raise Exception('property not found: ' + propertyName)


    # this method assumes that it is not passed handled props or
    # jvm props
    def gfshArg(self, key, val):
        if key in gemprops.GEMFIRE_PROPS:
            if self.isBindAddressProperty(key):
                val = self.translateBindAddress(val)
            
            return '--J=-Dgemfire.{0}={1}'.format(key,val)

        else:
            return '--J=-D{0}={1}'.format(key,val)


    def buildGfshArgs(self, props):
        result = []
        for key in props.keys():
            if not key in gemprops.HANDLED_PROPS:
                result.append(self.gfshArg(key, props[key]))
                
        return result
                            
    def processesOnThisHost(self, processType):
        if self.thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))
        
        result = []
        for processName in self.clusterDef['hosts'][self.thisHost]['processes'].keys():
            process = self.clusterDef['hosts'][self.thisHost]['processes'][processName]
            if process['type'] == processType:
                result.append(processName)
            
        return result

# public interface

    def locatorsOnThisHost(self):
        return self.processesOnThisHost('locator')


    def datanodesOnThisHost(self):
        return self.processesOnThisHost('datanode')


    def isLocator(self, processName):
        return self.isProcessOnThisHost(processName, 'locator')

    
    def isDatanode(self, processName):
        return self.isProcessOnThisHost(processName, 'datanode')


    def locatorProperty(self, processName, propertyName):
        result = self.processProperty('locator',processName, propertyName)
        if self.isBindAddressProperty(propertyName):
            return self.translateBindAddress(result)
        else:
            return result

        
    def datanodeProperty(self, processName, propertyName):
        result = self.processProperty('datanode',processName, propertyName)
        if self.isBindAddressProperty(propertyName):
            return self.translateBindAddress(result)
        else:
            return result
        
    def hasDatanodeProperty(self, processName, propertyName):
        pProps = self.processProps(processName)
        if propertyName in pProps:
            return True
        
        hostProps = self.hostProps()
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
    def hasLocatorProperty(self, processName, propertyName):
        pProps = self.processProps(processName)
        if propertyName in pProps:
            return True
        
        hostProps = self.hostProps()
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
            for k in source.keys():
                temp[k] = source[k]
                
        #now post-process, removing the items that cannot be passed as
        #-Ds and prefixing the remainders
        result = self.buildGfshArgs(temp)
        
        # now directly add the contents of jvm-options' if present
        if 'jvm-options' in temp:
            for option in temp['jvm-options']:
                result.append('--J={0}'.format(option))

        return result
        
        
        
    
    
