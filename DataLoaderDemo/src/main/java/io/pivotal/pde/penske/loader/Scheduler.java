/**
 * 
 */
package io.pivotal.pde.penske.loader;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.pivotal.pde.penske.unittelemetry.aggregator.model.UnitTelemetry;

/**
 * @author spaladugu
 *
 */
@Component
public class Scheduler {
	private static Log log = LogFactory.getLog(Scheduler.class.getName());

	@Autowired
	DataLoaderProperties applicationProperties;

	@Autowired
	LoadGenerator loadGenerator;

	@Autowired
	UnitTelemetryRepository unitTelemetryRepository;

	@Autowired
	UTLatestRepository utLatestRepository;

	@Scheduled(fixedRate = 1000)
	public void generateBatch() {
		log.debug("\n \t Generating UnitTelemteries ........");
		Set<UnitTelemetry> uts = loadGenerator.generate();
		//Set<UTLatest> utls = loadGenerator.generate1();
		log.debug("\n \t Saving UnitTelemteries ........");
		unitTelemetryRepository.save(uts);
		//utLatestRepository.save(utls);
		log.debug("Finished Batch ....");
	}
}
