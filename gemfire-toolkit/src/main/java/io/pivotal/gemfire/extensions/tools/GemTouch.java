package io.pivotal.gemfire.extensions.tools;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.management.DistributedSystemMXBean;

public class GemTouch {
	public static String NAME = "Touch";
	
	private static String jmxManagerHost = null;
	private static int jmxManagerPort = 0;
	private static int ratePerSecond = 0;
	private static String userName = null;
	private static String password = null;
	private static int regionCreationDelay = 20;
	
	private static String JMX_MANAGER_HOST_PREFIX="--jmx-manager-host=";
	private static String JMX_MANAGER_PORT_PREFIX="--jmx-manager-port=";
	private static String JMX_USERNAME_PREFIX="--jmx-username=";
	private static String JMX_PASSWORD_PREFIX="--jmx-password=";
	private static String RATE_PER_SECOND_PREFIX="--rate-per-second=";
	private static String METADATA_REGION_NAME_PREFIX="--metadata-region-name=";
	private static String REGION_CREATION_DELAY_PREFIX="--region-creation-delay=";
	
	private static String METADATA_REGION="/__regionAttributesMetadata";
	
	//TODO: 
	
	public static void main(String []args){
		int rc = 1;
		JMXConnector jmxc = null;
		try {
			parseArgs(args);
			
			JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + jmxManagerHost + ":" + jmxManagerPort + "/jmxrmi");
			HashMap<String, Serializable> env = null;
			if (userName != null){
				env = new HashMap<String,Serializable>();
				env.put(JMXConnector.CREDENTIALS, new String []{userName, password});
			}
			jmxc = JMXConnectorFactory.connect(url, env);	
			MBeanServerConnection mbsc= jmxc.getMBeanServerConnection();
			ObjectName oname = new ObjectName("GemFire:service=System,type=Distributed");
			DistributedSystemMXBean distributedSystemBean  = JMX.newMXBeanProxy(mbsc, oname, DistributedSystemMXBean.class);
			initCache(distributedSystemBean);	
			
			String []regionNames = distributedSystemBean.listAllRegionPaths();
			ArrayList<String> regionNameList = new ArrayList<String>(regionNames.length);
			for (String regionName : regionNames){
				if (regionName.equals(METADATA_REGION)){
					touchRegion(METADATA_REGION);
					
					if (regionCreationDelay > 0){
						System.out.println("waiting " + regionCreationDelay + " seconds for region creation");
						try{
							Thread.sleep(1000 * regionCreationDelay);
						} catch(InterruptedException x){
							// just continue
						}
					}
				} else {
					regionNameList.add(regionName);
				}
			}

			
			Collections.sort(regionNameList);
				
			for(String regionName: regionNameList){
				touchRegion(regionName);
			}
			
			rc = 0;
			
		} catch(Exception x){
			x.printStackTrace(System.err);
		} finally {
			if (jmxc != null) {
				try {
					jmxc.close();
					System.out.println("closed JMX connection");
				} catch(IOException iox){
					System.err.println("warning: error closing jmx connection");
				}
			}
			
			ClientCache cache = ClientCacheFactory.getAnyInstance();
			if (cache != null) cache.close();
		}
		
		System.exit(rc);
	}
	
	private static void touchRegion(String regionName){
		Region<Object,Object> r = getRegion(regionName);
		
		TouchAllArgs touchAllArgs = new TouchAllArgs();
		touchAllArgs.setRatePerSecond(ratePerSecond);
		Execution exec = FunctionService.onRegion(r).withArgs(touchAllArgs).withCollector(new LoggingResultCollector());
		ResultCollector<String,String> results = (ResultCollector<String,String>) exec.execute(GemTouch.NAME);
		results.getResult();
		System.out.println("finished touch for " + r.getFullPath());	
	}
	
	private static void initCache(DistributedSystemMXBean dsBean){
		String locatorString = dsBean.listLocators()[0];
		ClientCacheFactory factory = new ClientCacheFactory();
		setupPools(factory, locatorString);
		factory.create();
		System.out.println("connected to distributed system  with locator " + dsBean.listLocators()[0]);
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
		if (args.length < 2) {
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
			} else if (arg.startsWith(JMX_USERNAME_PREFIX)){
				userName = arg.substring(JMX_USERNAME_PREFIX.length());
			} else if (arg.startsWith(JMX_PASSWORD_PREFIX)){
				password = arg.substring(JMX_PASSWORD_PREFIX.length());
			} else if (arg.startsWith(RATE_PER_SECOND_PREFIX)){
				String s = arg.substring(RATE_PER_SECOND_PREFIX.length());
				try {
					ratePerSecond = Integer.parseInt(s);
				} catch(NumberFormatException x){
					System.err.println("--rate-per-second must be an integer: " + s);
					System.exit(1);
				}
			} else if (arg.startsWith(REGION_CREATION_DELAY_PREFIX)){
				String s = arg.substring(REGION_CREATION_DELAY_PREFIX.length());
				try {
					regionCreationDelay = Integer.parseInt(s);
				} catch(NumberFormatException x){
					System.err.println("--region-creation-delay must be an integer: " + s);
					System.exit(1);
				}
			} else if (arg.startsWith(METADATA_REGION_NAME_PREFIX)){
				METADATA_REGION = arg.substring(METADATA_REGION_NAME_PREFIX.length());
				if (! METADATA_REGION.startsWith("/")){
					System.err.println("--metadata-region-name must start with \"/\"");
					System.exit(1);
				}
			} else {
				System.err.println("unrecognized argument: " + arg);
				System.exit(1);
			}
		}
		
		if (jmxManagerHost == null){
			System.err.println("--jmx-manager-host is required");
			System.exit(1);
		}
		
		if (jmxManagerPort == 0){
			System.err.println("--jmx-manager-port is required");
			System.exit(1);
		}
		
		if ( (userName != null) || (password != null)){
			if ( (userName == null) || (password == null)){
				System.out.println("either --jmx-username and --jmx-password must both be provided or neither may be provided");
				System.exit(1);
			}
		}
		
		if (ratePerSecond < 0){
			System.err.println("rate per second must be greater than equal 0 if provided");
			System.exit(1);
		}
	}
	
	private static void printUsage(){
		System.err.println("usage: gemtouch --jmx-manager-host=abc --jmx-manager-port=123 --jmx-username=fred --jmx-password=pass --rate-per-thread=100");
		System.err.println("\t--jmx-manager-host and --jmx-manager-port must point to a GemFire jmx manager (usually the locator)");
		System.err.println("\t\tif you are not sure of the port number try 1099");		
		System.err.println("\t--jmxusername and --jmx-manager-password are optional but if either is present the other must also be provided");
		System.err.println("\t--rate-per-second is optional - acts a a throttle if present");
		System.err.println();
		System.err.println("\tif the metadata region \"" + METADATA_REGION + "\" is present it will be touched first ");
		System.err.println("\tthe name of the metadata region can be set with the --metadata-region-name option");
		System.err.println("\tafter touching the metadata region the program will pause for 20s to allow for propagation");
		System.err.println("\tthe length of the wait (in seconds)  can be set using the --region-creation-delay option");
	}
 }
