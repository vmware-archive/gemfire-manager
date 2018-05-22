package io.pivotal.pde.peoplecounter;

import java.util.List;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.pdx.PdxInstance;

public class PeopleCounter implements AsyncEventListener, Declarable {

	private static String SUMMARY_REGION = "dailyAccountStatus";
	private static String VERSION = "03";
		
	@Override
	public void close() {
	}

	@Override
	public boolean processEvents(List<AsyncEvent> events) {
		CacheFactory.getAnyInstance().getLogger().error(">>> PeopleCounter v" + PeopleCounter.VERSION + " processing batch of " + events.size() + " events.");
		Region<String,Integer> summaryRegion = CacheFactory.getAnyInstance().getRegion(SUMMARY_REGION);
		CacheTransactionManager tm = CacheFactory.getAnyInstance().getCacheTransactionManager();
		boolean result = false;
		tm.begin();
		try {
			for (AsyncEvent<Integer,PdxInstance> event: events){
				processEvent(event.getDeserializedValue(), summaryRegion);
			}
			
			tm.commit();
			result = true;
		} catch(Exception x){
			if (tm.exists()) tm.rollback();
			CacheFactory.getAnyInstance().getLogger().error(">>> Error Processing Batch " + x.getMessage());
			x.printStackTrace(System.err);
		}
		
		return result;
	}
	
	private void processEvent(PdxInstance pdx, Region<String,Integer> summaryRegion){
		PdxInstance address = (PdxInstance) pdx.getField("address");
		String state = (String) address.getField("state");
		Integer prev = summaryRegion.putIfAbsent(state, Integer.valueOf(1));
		if (prev != null){
			prev = summaryRegion.get(state);
			summaryRegion.put(state, Integer.valueOf(prev.intValue() + 1));
		}
	}

}
