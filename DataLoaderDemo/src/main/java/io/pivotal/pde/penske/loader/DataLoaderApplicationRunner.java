/**
 * 
 */
package io.pivotal.pde.penske.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @author spaladugu
 *
 */
@Component

public class DataLoaderApplicationRunner  implements ApplicationRunner  {

	Log log = LogFactory.getLog(getClass());
	@Autowired
	DataLoaderProperties applicationProperties;


	@Override
	public void run(ApplicationArguments args) throws Exception {
		log.debug("< ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		log.debug("DataLoaderApp Runtime Non-Options :");
		for (String nonOption : args.getNonOptionArgs()) {
			log.debug(nonOption);
		}
		log.debug("DataLoaderApp Runtime Options :");
		for (String option : args.getOptionNames()) {
			log.debug(option + " = " + args.getOptionValues(option));
		}
		log.debug("< ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
	}
}
