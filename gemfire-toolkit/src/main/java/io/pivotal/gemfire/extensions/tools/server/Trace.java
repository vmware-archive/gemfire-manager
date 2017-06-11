package io.pivotal.gemfire.extensions.tools.server;


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

public class Trace implements Function {

	
	
	@Override
	public void execute(FunctionContext ctx) {
		String regionName = (String) ctx.getArguments();
		
		Region region = CacheFactory.getAnyInstance().getRegion(regionName);
		if (region == null){
			throw new RuntimeException("region not found: " + regionName);
		}
		
		String result = null;
		
		RegionAttributes attrs = region.getAttributes();
		for (CacheListener listener : attrs.getCacheListeners()){
			if (listener instanceof TraceCacheListener){
				result = "trace listener already present on " + regionName + " in " + CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember().getName();				
				break;
			}
		}
		
		if (result == null){
			CacheListener l = new TraceCacheListener(regionName);
			region.getAttributesMutator().addCacheListener(l);
			result = "trace listener installed  on " + regionName + " in " + CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember().getName();				
		}

		ctx.getResultSender().lastResult(result);
	}

	
	@Override
	public String getId() {
		return io.pivotal.gemfire.extensions.tools.Trace.NAME;
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
