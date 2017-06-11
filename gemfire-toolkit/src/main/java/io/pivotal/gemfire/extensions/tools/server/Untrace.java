package io.pivotal.gemfire.extensions.tools.server;


import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

/**
 * optionally takes a list of region names
 * 
 * 
 * @author wmay
 *
 */

public class Untrace implements Function {

	
	
	@Override
	public void execute(FunctionContext ctx) {
		String regionName = (String) ctx.getArguments();
		
		Region region = CacheFactory.getAnyInstance().getRegion(regionName);
		if (region == null){
			throw new RuntimeException("region not found: " + regionName);
		}
		
		
		List<CacheListener> listenersToRemove = new ArrayList<CacheListener>(1);
		
		RegionAttributes attrs = region.getAttributes();
		for (CacheListener listener : attrs.getCacheListeners()){
			if (listener instanceof TraceCacheListener){
				listenersToRemove.add(listener);
			}
		}
		
		String result = null;
		if (listenersToRemove.size() == 0){
			result = "trace listener not found on " + regionName + " in " + CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember().getName();				
		} else {
			AttributesMutator ram = region.getAttributesMutator();
			for (CacheListener l : listenersToRemove){
				ram.removeCacheListener(l);
			}
			result = "trace listener removed from " + regionName + " in " + CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember().getName();							
		}

		ctx.getResultSender().lastResult(result);
	}

	
	@Override
	public String getId() {
		return io.pivotal.gemfire.extensions.tools.Untrace.NAME;
	}

	@Override
	public boolean hasResult() {
		return true;
	}

	@Override
	public boolean isHA() {
		return true;
	}

	@Override
	public boolean optimizeForWrite() {
		return true;
	}

}
