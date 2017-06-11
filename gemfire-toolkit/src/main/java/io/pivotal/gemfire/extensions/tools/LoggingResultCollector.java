package io.pivotal.gemfire.extensions.tools;

import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

public class LoggingResultCollector implements ResultCollector <String,String> {

	@Override
	public void addResult(DistributedMember mbr, String msg) {
		System.out.println(mbr.getName() +  " on " + mbr.getHost()  + " " + msg);
	}

	@Override
	public void clearResults() {
	}

	@Override
	public void endResults() {
	}

	@Override
	public String getResult() throws FunctionException {
		return "SUCCESS";
	}

	@Override
	public String getResult(long arg0, TimeUnit arg1) throws FunctionException,
			InterruptedException {
		return "SUCCESS";
	}

}
