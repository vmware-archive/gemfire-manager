package io.pivotal.gemfire.extensions.tools;

import java.io.Serializable;

public class TouchAllArgs implements Serializable {
	private static final long serialVersionUID = 1251714908388691718L;
	
	private int ratePerSecond;
	
	public TouchAllArgs(){
		// set defaults
		ratePerSecond = 0;
	}

	public int getRatePerSecond() {
		return ratePerSecond;
	}

	public void setRatePerSecond(int ratePerSecond) {
		this.ratePerSecond = ratePerSecond;
	}

}

