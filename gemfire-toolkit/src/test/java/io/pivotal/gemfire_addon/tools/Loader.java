package io.pivotal.gemfire_addon.tools;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;

public class Loader {
	public static void main(String []args){
		
		
		if(args.length == 0)
		{
			System.err.println("Usage java "+Loader.class.getSimpleName()+" locator-host");
			System.exit(-1);
		}
		
		ClientCache cache = null;
		try {
			
			String locatorHost = args[0];
			
			System.out.println("Connecting to "+locatorHost+"[10000]");
			cache = new ClientCacheFactory().addPoolLocator(locatorHost, 10000).create();
			Region<Object,Object> testRegion = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("Test");
			
			for(int i=0; i< 1000; ++i){
				testRegion.put(Integer.valueOf(i), Integer.valueOf(i));
			}
			
		} catch(Exception x){
			x.printStackTrace(System.err);
		} finally {
			if (cache != null) cache.close();
		}
	}
}
