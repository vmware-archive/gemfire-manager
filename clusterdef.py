import gemprops
import netifaces
import os
import socket


class ClusterDef:
    
    def __init__(self, cdef):
        self.clusterDef = cdef
        self.thisHost = socket.gethostname()

     
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
    def processProps(self, processName):
        processes = None
        if self.thisHost in self.clusterDef['hosts']:
            processes = self.clusterDef['hosts'][self.thisHost]['processes']
        
        elif 'localhost' in self.clusterDef['hosts']:
            processes = self.clusterDef['hosts']['localhost']['processes']
            
        else:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))
                    
        return processes[processName]

    
    #host props are optional - if they are not defined in the file an empty
    #dictionary will be returned
    def hostProps(self):
        result = dict()
        if self.thisHost  in self.clusterDef['hosts']:
            result = self.clusterDef['hosts'][self.thisHost]['host-properties']
            
        elif 'localhost'  in self.clusterDef['hosts']:
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
        if self.isBindAddressProperty(key):
            val = self.translateBindAddress(val)
            
        if key in gemprops.GEMFIRE_PROPS:
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
        result = []
        for hostname in [self.thisHost, 'localhost']:        
            if hostname in self.clusterDef['hosts']:        
                for processName in self.clusterDef['hosts'][hostname]['processes'].keys():
                    process = self.clusterDef['hosts'][hostname]['processes'][processName]
                    if process['type'] == processType:
                        result.append(processName)
            
        return result

# public interface

    def locatorsOnThisHost(self):
        return self.processesOnThisHost('locator')


    def datanodesOnThisHost(self):
        return self.processesOnThisHost('datanode')


    def isLocatorOnThisHost(self, processName):
        return self.isProcessOnThisHost(processName, 'locator')

    
    def isDatanodeOnThisHost(self, processName):
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
        
        
        
    
    
