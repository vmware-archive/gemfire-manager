package io.pivotal.gemfire.extensions.tools;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.geode.management.DistributedSystemMXBean;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

public class JMXUtil {
	
	private static String jmxManagerHost = null;
	private static int jmxManagerPort = 0;
	private static String jmxManagers = null;
	private static String userName = null;
	private static String password = null;
	
	private static String JMX_MANAGERS_PREFIX="--jmx-managers=";
	private static String JMX_MANAGER_HOST_PREFIX="--jmx-manager-host=";
	private static String JMX_MANAGER_PORT_PREFIX="--jmx-manager-port=";
	private static String JMX_USERNAME_PREFIX="--jmx-username=";
	private static String JMX_PASSWORD_PREFIX="--jmx-password=";

			
	public static void main(String []args){
		int rc = 1;
		JMXConnector jmxc = null;
		try {
			int i = parseArgs(args);
			
			if (jmxManagers == null)
				jmxc = singleTargetConnect(jmxManagerHost, jmxManagerPort);
			else
				jmxc = multipleTargetConnect();
			
			MBeanServerConnection mbsc= jmxc.getMBeanServerConnection();
			ObjectName dsOname = new ObjectName("GemFire:service=System,type=Distributed");
			DistributedSystemMXBean distributedSystemBean  = JMX.newMXBeanProxy(mbsc, dsOname, DistributedSystemMXBean.class);
	
			if ( args.length <= i){
				System.err.println("no command given, exiting");
				System.exit(1);
			}
			
			String cmd = args[i++];
			
			if (cmd.equals("list")){
				if ( args.length <= i){
					System.err.println("no list target given, exiting");
					System.exit(1);
				}
				
				String tgt = args[i++];
				if (tgt.equals("disk-stores")){
					listDiskStores(distributedSystemBean); 
					rc = 0;
				} else {
					System.err.println("unrecognized command: " + cmd);
					System.exit(1);
				}
			} else {
				System.err.println("unrecognized command: " + cmd);
				System.exit(1);
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

	private static void listDiskStores(DistributedSystemMXBean distributedSystemBean) throws IOException{
		Map<String, String[]> result = distributedSystemBean.listMemberDiskstore();
		
		JsonWriter writer = new JsonWriter(new OutputStreamWriter(System.out));
		writer.setIndent("   ");
		writer.beginObject();
		writer.name("members");
		writer.beginArray();
		for(String member: result.keySet()){
			writer.beginObject();
			writer.name("member-name").value(member);
			writer.name("disk-stores");
			writer.beginArray();
			for(String dstore: result.get(member)) writer.value(dstore);
			writer.endArray();
			writer.endObject();
		}
		writer.endArray();
		writer.endObject();
		writer.close();
	}
	
	//returns the index of the first unrecognized argument, which should be a command verb
	private static int parseArgs(String []args){
		if (args.length < 1) {
			printUsage();
			System.exit(1);
		}
		
		int i=0;
		for(; i < args.length; ++i){
			String arg = args[i];

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
			} else {
				break;
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
				System.err.println("either --jmx-username and --jmx-password must both be provided or neither may be provided");
				System.exit(1);
			}
		}
		
		return i;
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
		System.err.println("usage: jmxutil --jmx-manager-host=abc --jmx-manager-port=123 --jmx-username=fred --jmx-password=pass <commands>");
		System.err.println("\t--jmx-manager-host and --jmx-manager-port must point to a GemFire jmx manager (usually the locator)");
		System.err.println("\t\tif you are not sure of the port number try 1099");		
		System.err.println("\t--jmx-managers specifies a comma separated list of jmx managers using host:port notation");
		System.err.println("\t--jmxusername and --jmx-manager-password are optional but if either is present the other must also be provided");
		System.err.println();
		System.err.println("commands can be one of the following");
		System.err.println("\tlist disk-stores");
	}
 
}
