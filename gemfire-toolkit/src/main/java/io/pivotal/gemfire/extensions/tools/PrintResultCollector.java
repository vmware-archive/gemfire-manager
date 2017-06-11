package io.pivotal.gemfire.extensions.tools;

import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

/*
 * simply writes messages to the console as they arrive
 */
public class PrintResultCollector implements ResultCollector<String, String> {

	@Override
	public void addResult(DistributedMember member, String msg) {
		System.out.println(msg);
	}

	@Override
	public void clearResults() {
	}

	@Override
	public void endResults() {
	}

	@Override
	public String getResult() throws FunctionException {
		return "done";
	}

	@Override
	public String getResult(long arg0, TimeUnit arg1) throws FunctionException,
			InterruptedException {
		return "done";
	}
	
}
