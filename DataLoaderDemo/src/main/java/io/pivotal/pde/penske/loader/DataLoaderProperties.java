/**
 * 
 */
package io.pivotal.pde.penske.loader;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author spaladugu
 *
 */
@Component
@ConfigurationProperties(prefix = "UTLoaderApp")
public class DataLoaderProperties {
	int batchsize;

	public int getBatchsize() {
		return batchsize;
	}

	public void setBatchsize(int batchsize) {
		this.batchsize = batchsize;
	}
	
}
