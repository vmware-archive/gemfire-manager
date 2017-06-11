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
import org.apache.geode.management.MemberMXBean;

public class ListPIDs {
	public static String NAME = "Touch";
	
	private static String jmxManagerHost = null;
	private static int jmxManagerPort = 0;
	private static String userName = null;
	private static String password = null;
	private static boolean verbose = false;
	
	private static String JMX_MANAGER_HOST_PREFIX="--jmx-manager-host=";
	private static String JMX_MANAGER_PORT_PREFIX="--jmx-manager-port=";
	private static String JMX_USERNAME_PREFIX="--jmx-username=";
	private static String JMX_PASSWORD_PREFIX="--jmx-password=";
	private static String VERBOSE_FLAG="--verbose";

	
	public static void main(String []args){
		int rc = 0;
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
			ObjectName dsOname = new ObjectName("GemFire:service=System,type=Distributed");
			DistributedSystemMXBean distributedSystemBean  = JMX.newMXBeanProxy(mbsc, dsOname, DistributedSystemMXBean.class);

			ObjectName []memberObjectNames = distributedSystemBean.listMemberObjectNames();
			for(ObjectName moName : memberObjectNames){
				MemberMXBean member = JMX.newMBeanProxy(mbsc, moName, MemberMXBean.class);
				if (member.isServer()){
					System.out.println(member.getName() + " " + member.getHost() + " " + member.getProcessId());
				}
			}
			
		} catch(Exception x){
			x.printStackTrace(System.err);
			rc = 1;
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
			} else if (arg.equals(VERBOSE_FLAG)){
				verbose = true;
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
		
	}
	
	private static void printUsage(){
		System.err.println("usage: listpids --jmx-manager-host=abc --jmx-manager-port=123 --jmx-username=fred --jmx-password=pass ");
		System.err.println("\t--jmx-manager-host and --jmx-manager-port must point to a GemFire jmx manager (usually the locator)");
		System.err.println("\t\tif you are not sure of the port number try 1099");		
		System.err.println("\t--jmxusername and --jmx-manager-password are optional but if either is present the other must also be provided");
		System.err.println();
		System.err.println("\tlists cache server hosts and pids (locator pids are not included)");
	}
 
}
