package io.pivotal.gemfire.extensions.tools.server;

import io.pivotal.gemfire.extensions.tools.TouchAllArgs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.partition.PartitionRegionHelper;

/**
 * optionally takes a list of region names
 * 
 * 
 * @author wmay
 *
 */

public class Touch implements Function {

	private static final long serialVersionUID = 8827164389473146995L;

	private static int BATCHSIZE=100;
	private static long REPORT_INTERVAL_MS = 10l * 1000l;
	
	@Override
	public void execute(FunctionContext ctx) {
		RegionFunctionContext rctx = (RegionFunctionContext) ctx;
		TouchAllArgs args = (TouchAllArgs) rctx.getArguments();
		Region<Object,Object> region = rctx.getDataSet();
		if (region.getAttributes().getDataPolicy().withPartitioning() ){
			region = PartitionRegionHelper.getLocalDataForContext(rctx);
		}
		
		Object []keys = region.keySet().toArray();
		int i=0;
		Invocation invocation = new Invocation(region.getFullPath(), keys.length);
		for(;i+BATCHSIZE < keys.length; i+=BATCHSIZE){
			processBatch(invocation, args, region, Arrays.copyOfRange(keys,i,i+BATCHSIZE), rctx.<String>getResultSender());
		}
		// left over batch
		if (i <keys.length){
			processBatch(invocation, args, region,Arrays.copyOfRange(keys,i,keys.length), rctx.<String>getResultSender());
		}
		
		invocation.lastReport( rctx.<String>getResultSender());
	}

	private void processBatch(Invocation invocation, TouchAllArgs args,  Region<Object,Object> region, Object[]keys, ResultSender<String> resultSender){
		// introduce sleep as necessary to throttle to the desired rate
		long targetRate = args.getRatePerSecond();
		if ( targetRate > 0){
			long currentRate = invocation.getTouchesPerSecond();
			if (currentRate > args.getRatePerSecond()){
				long targetElapsedMs = (invocation.getTouched() * 1000) / targetRate;
				long sleep = targetElapsedMs - invocation.getElapsedMs();
				if (sleep > 0){
					try {
						Thread.sleep(sleep);
					} catch(InterruptedException x){
						// not a problem
					}
				}
			}
		}
		
		boolean copyOnRead = CacheFactory.getAnyInstance().getCopyOnRead();
		// do the touch using transaction semantics so we will not accidentally
		// undo an update that is happening concurrently
		CacheTransactionManager tm = CacheFactory.getAnyInstance().getCacheTransactionManager();
		tm.begin();
		try {
			for(Object key: keys) putGet(region,key, !copyOnRead);
			tm.commit();
			tm = null;
		} catch(CommitConflictException x){
			processBatchOneAtATime(region, keys);
		} finally {
			if (tm != null) tm.rollback();
			tm = null;
		}
		invocation.incrementTouched(keys.length);
		
		// now assess whether we need to send back a status report / log a message
		if (invocation.getTimeSinceLastReport() > REPORT_INTERVAL_MS) invocation.report(resultSender);
	}

	private void processBatchOneAtATime(Region<Object,Object> region, Object[]keys){
		// do the touch using transaction semantics so we will not accidentally
		// undo an update that is happening concurrently
		boolean copyOnRead = CacheFactory.getAnyInstance().getCopyOnRead();
		for(Object key : keys){
			CacheTransactionManager tm = CacheFactory.getAnyInstance().getCacheTransactionManager();
			tm.begin();
			try {
				putGet(region, key, !copyOnRead);
				tm.commit();
				tm = null;
			} catch(CommitConflictException x){
				// this is OK - it just means someone else updated the key and we don't want to overwrite it
			} finally {
				if (tm != null) tm.rollback();
				tm = null;
			}
		}
	}	

	private void putGet(Region<Object,Object> region, Object key, boolean copy){
		Object val = region.get(key);
		
		if (val != null){
			if (copy){
				key = CopyHelper.copy(key);
				val = CopyHelper.copy(val);
			}
			
			region.put(key, val);
		}
	}
	
	private Map<Object,Object> copyMap(Map<Object,Object> map){
		Map<Object,Object> result = new HashMap<Object,Object>(map.size());
		
		for(Entry<Object, Object> entry : result.entrySet()){
			Object key = CopyHelper.copy(entry.getKey());
			Object val = CopyHelper.copy(entry.getValue());
			result .put(key, val);
		}
		
		return result;
	}
	
	@Override
	public String getId() {
		return io.pivotal.gemfire.extensions.tools.GemTouch.NAME;
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

	// I'm not certain whether / how Function instances are re-uses within the server so
	// all invocation related state will be stored in a newly allocated instance of Invocation.
	// Since there will be one instance per invocation it does not need to be thread safe.
	private static class Invocation {
		private String regionName;
		private long totalEntries;
		private long touched;
		private long startTime;
		private long lastReport;
		
		public Invocation(String regionName, long totalEntries){
			this.regionName = regionName;
			this.totalEntries = totalEntries;
			touched = 0l;
			lastReport = 0l;
			startTime = System.currentTimeMillis();
		}
		
		public void incrementTouched(long i){
			touched += i;
		}
		
		public long getTouched(){
			return touched;
		}
		
		public long getElapsedMs(){
			return System.currentTimeMillis() - startTime;
		}
		
		public long getTouchesPerSecond(){
			long elapsed = this.getElapsedMs();
			if (elapsed == 0) return 0; // avoids divide by zero
			
			return (this.getTouched() * 1000) / elapsed;
		}
		
		public long getTimeSinceLastReport(){
			return System.currentTimeMillis() - lastReport;
		}
		
		public void report(ResultSender<String> resultSender){
			String msg = "touched " + touched + "/" + totalEntries + " entries in " + regionName;
			CacheFactory.getAnyInstance().getLogger().info(msg);
			lastReport = System.currentTimeMillis();
			resultSender.sendResult(msg);
		}
		
		public void lastReport(ResultSender<String> resultSender){
			String msg = "FINISHED: touched " + touched + "/" + totalEntries + " entries in " + regionName;
			CacheFactory.getAnyInstance().getLogger().info(msg);
			lastReport = System.currentTimeMillis();
			resultSender.lastResult(msg);
		}
	}
}
