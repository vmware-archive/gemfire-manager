package io.pivotal.gemfire.extensions.tools;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;

public class CheckRedundancy {
	public static String NAME = "Touch";
	
	private static String jmxManagerHost = null;
	private static int jmxManagerPort = 0;
	private static String jmxManagers = null;
	private static String userName = null;
	private static String password = null;
	private static boolean verbose = false;
	private static int wait = 0;
	
	private static String JMX_MANAGERS_PREFIX="--jmx-managers=";
	private static String JMX_MANAGER_HOST_PREFIX="--jmx-manager-host=";
	private static String JMX_MANAGER_PORT_PREFIX="--jmx-manager-port=";
	private static String JMX_USERNAME_PREFIX="--jmx-username=";
	private static String JMX_PASSWORD_PREFIX="--jmx-password=";
	private static String WAIT_PREFIX="--wait=";
	private static String VERBOSE_FLAG="--verbose";

	private static HashMap<ObjectName, DistributedRegionMXBean> regionBeans;
	
	
	public static void main(String []args){
		int rc = 1;
		JMXConnector jmxc = null;
		try {
			regionBeans = new HashMap<ObjectName, DistributedRegionMXBean>(200);
			
			parseArgs(args);
			
			if (jmxManagers == null)
				jmxc = singleTargetConnect(jmxManagerHost, jmxManagerPort);
			else
				jmxc = multipleTargetConnect();
			
			MBeanServerConnection mbsc= jmxc.getMBeanServerConnection();
			ObjectName dsOname = new ObjectName("GemFire:service=System,type=Distributed");
			DistributedSystemMXBean distributedSystemBean  = JMX.newMXBeanProxy(mbsc, dsOname, DistributedSystemMXBean.class);
	
			int atRiskRegions = 0;
			long start = System.currentTimeMillis();
			long elapsed = 0;

			while( wait == 0 || elapsed < (1000 * wait)){
				updateRegionMap(distributedSystemBean, mbsc);

				atRiskRegions = checkAllRegions();
				if (atRiskRegions == 0) break; //BREAK
				if (wait == 0) break; //BREAK
				
				try {
					Thread.sleep(2000);
				} catch(InterruptedException x){
					// not a problem
				}
				elapsed = System.currentTimeMillis() - start;
			}
			
			if (atRiskRegions == 0){
				System.out.println("all partitioned regions have redundancy");
				rc = 0;
			} 			
			
		} catch(Exception x){
			x.printStackTrace(System.err);
		} finally {
			if (jmxc != null) {
				try {
					jmxc.close();
				} catch(IOException iox){
					System.err.println("warning: error closing jmx connection");
				}
			}			
		}
		
		System.exit(rc);
	}

	private static int checkAllRegions(){
		ObjectName []objects = new ObjectName[regionBeans.size()]; 
		regionBeans.keySet().toArray(objects);
		int atRiskRegions = 0;
		
		Arrays.sort(objects, new ObjectNameComparator());
		for( ObjectName oname: objects){
			DistributedRegionMXBean regionMBean = regionBeans.get(oname);
			String type = regionMBean.getRegionType();
			if (type.contains("PARTITION")){
				int count = regionMBean.getNumBucketsWithoutRedundancy();
				if (count > 0 || verbose ){
					if (count > 0) ++atRiskRegions;
					System.out.println(oname.toString() + " " + regionMBean.getFullPath() + " : " + count  + " buckets without redundancy");
				} 
			} else {
				if (verbose) {
					System.out.println(oname.toString() + " " + regionMBean.getFullPath() + " : NA (not partitioned)");
				}
			}
		}
		
		return atRiskRegions;
	}
	
	private static void updateRegionMap(DistributedSystemMXBean distributedSystemBean,MBeanServerConnection mbsc ){
		ObjectName []objects = distributedSystemBean.listDistributedRegionObjectNames();
		
		// remove any regions that no longer exist
		Set<ObjectName> keys = regionBeans.keySet();
		for(ObjectName key : keys){
			boolean found = false;
			for(ObjectName existingObject : objects){
				if (key.equals(existingObject)){
					found = true;
					break;
				}
			}
			if (!found) regionBeans.remove(key);
		}
		
		// add any new
		for(ObjectName existingObject : objects){
			if (! regionBeans.containsKey(existingObject)){
				DistributedRegionMXBean regionMBean = JMX.newMXBeanProxy(mbsc, existingObject, DistributedRegionMXBean.class);
				regionBeans.put(existingObject, regionMBean);
			}
		}
	}
	
	// this will need to be enhanced to support server groups
	private static void setupPools(ClientCacheFactory ccf, String locator){
		Pattern pattern = Pattern.compile("(.*)\\[(.*)\\]");
		Matcher matcher = pattern.matcher(locator);
		
		if (!matcher.matches())
			throw new RuntimeException("unexpected exception: could not parse locator string retrieved from distributed system: " + locator);
		
		String host = matcher.group(1);
		int port = Integer.parseInt(matcher.group(2));
		
		ccf.addPoolLocator(host, port);
	}
	
	private static Region<Object,Object> getRegion(String name){
		Region<Object,Object> result = ClientCacheFactory.getAnyInstance().getRegion(name);
		if (result != null) return result; // RETURN
		
		// otherwise need to create whatever doesn't yet exist
		int startIndex = 1;
		int endIndex = name.indexOf("/", startIndex);
		if (endIndex == -1) endIndex = name.length();
		
		result = getOrCreateRegion(name.substring(startIndex,endIndex));
		
		while(endIndex < name.length()){
			startIndex = endIndex + 1;
			endIndex = name.indexOf("/", startIndex);
			if (endIndex == -1) endIndex = name.length();
			
			result = getOrCreateSubregion(result, name.substring(startIndex, endIndex));
		}
		
		return result;
	}
	
	private static Region<Object,Object> getOrCreateRegion(String name){
		Region<Object,Object> result = ClientCacheFactory.getAnyInstance().getRegion(name);
		if (result == null) result = ClientCacheFactory.getAnyInstance().createClientRegionFactory(ClientRegionShortcut.PROXY).create(name);
		return result;
	}
	
	private static Region<Object,Object> getOrCreateSubregion(Region<Object,Object> parentRegion, String name){
		Region<Object,Object> result = ClientCacheFactory.getAnyInstance().getRegion(parentRegion.getFullPath() + "/" + name);
		if (result == null) result = ClientCacheFactory.getAnyInstance().createClientRegionFactory(ClientRegionShortcut.PROXY).createSubregion(parentRegion, name);
		return result;
	}
	
	private static void parseArgs(String []args){
		if (args.length < 1) {
			printUsage();
			System.exit(1);
		}
		
		for(String arg: args){
			if (arg.startsWith(JMX_MANAGER_HOST_PREFIX)) {
				jmxManagerHost = arg.substring(JMX_MANAGER_HOST_PREFIX.length());
			} else if (arg.startsWith(JMX_MANAGER_PORT_PREFIX)){
				String s = arg.substring(JMX_MANAGER_PORT_PREFIX.length());
				try {
					jmxManagerPort = Integer.parseInt(s);
				} catch(NumberFormatException x){
					System.err.println("--jmx-manager-port value must be an integer: " + s);
					System.exit(1);
				}
			} else if (arg.startsWith(JMX_MANAGERS_PREFIX)){
				jmxManagers = arg.substring(JMX_MANAGERS_PREFIX.length());
			} else if (arg.startsWith(JMX_USERNAME_PREFIX)){
				userName = arg.substring(JMX_USERNAME_PREFIX.length());
			} else if (arg.startsWith(JMX_PASSWORD_PREFIX)){
				password = arg.substring(JMX_PASSWORD_PREFIX.length());
			} else if (arg.equals(VERBOSE_FLAG)){
				verbose = true;
			} else if (arg.startsWith(WAIT_PREFIX)){
				String s = arg.substring(WAIT_PREFIX.length());
				try {
					wait = Integer.parseInt(s);
				} catch(NumberFormatException x){
					System.err.println("--wait value must be an integer: " + s);
					System.exit(1);
				}
			} else {
				System.err.println("unrecognized argument: " + arg);
				System.exit(1);
			}
		}
		
		if (jmxManagerHost == null && jmxManagers == null){
			System.err.println("--jmx-manager-host is required if --jmx-managers is not given");
			System.exit(1);
		}
		
		if (jmxManagerPort == 0  && jmxManagers == null){
			System.err.println("--jmx-manager-port is required if --jmx-managers is not given");
			System.exit(1);
		}
		
		if ( (userName != null) || (password != null)){
			if ( (userName == null) || (password == null)){
				System.out.println("either --jmx-username and --jmx-password must both be provided or neither may be provided");
				System.exit(1);
			}
		}
		
	}
	
	private static JMXConnector singleTargetConnect(String host, int port) throws  IOException {
		JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
		HashMap<String, Serializable> env = null;
		if (userName != null){
			env = new HashMap<String,Serializable>();
			env.put(JMXConnector.CREDENTIALS, new String []{userName, password});
		}
		JMXConnector jmxc = JMXConnectorFactory.connect(url, env);	
		return jmxc;
	}
	
	private static JMXConnector multipleTargetConnect() throws IOException {
		JMXConnector jmxc = null;
		String []targets = jmxManagers.split(",");
		for (String target: targets){
			int i = target.indexOf(':');
			if (i == -1 || i == (target.length() - 1))
				throw new RuntimeException("invalid format for --jmx-managers argument - must be a comma separated list of host:port - argument given was : \"" + jmxManagers + "\"");
			
			String host = target.substring(0,i);
			int port = 0;
			try {
				port = Integer.parseInt(target.substring(i + 1));
			} catch (NumberFormatException x){
				throw new RuntimeException("invalid format for --jmx-managers argument - port argument must be a number - argument given was : \"" + jmxManagers + "\"");
			}
			
			try {
				jmxc = singleTargetConnect(host, port);
				break;
			} catch (IOException iox){
				System.out.println("warning: could not connect to jmx manager at " + host + ":" + port);
			}
		}
		
		if (jmxc == null)
			throw new RuntimeException("could not connect to any jmx manager in the list: " + jmxManagers);
		
		return jmxc;
	}
	
	private static void printUsage(){
		System.err.println("usage: checkredundancy --jmx-manager-host=abc --jmx-manager-port=123 --jmx-username=fred --jmx-password=pass ");
		System.err.println("alternative usage: checkredundancy --jmx-managers=host1:port1,host2:port2 --jmx-username=fred --jmx-password=pass ");
		System.err.println("\t--jmx-manager-host and --jmx-manager-port must point to a GemFire jmx manager (usually the locator)");
		System.err.println("\t\tif you are not sure of the port number try 1099");		
		System.err.println("\t--jmx-managers specifies a comma separated list of jmx managers using host:port notation");
		System.err.println("\t--jmxusername and --jmx-manager-password are optional but if either is present the other must also be provided");
		System.err.println();
		System.err.println("\tcheckredundancy will return with a 0 exit code if all partition regions have redundancy");
		System.err.println("\tcheckredundancy will return with a 1 exit code if any partition regions do not have redundancy");
		System.err.println();		
		System.err.println("\tBy default, checkredundancy will report only partition regions that do not have redundancy");
		System.err.println("\t--verbose will cause checkredundancy to report redundancy of all regions");
		System.err.println("\t--wait=20 will cause checkredundancy to wait up to 20s for redundancy to be established");
	}
 
}
