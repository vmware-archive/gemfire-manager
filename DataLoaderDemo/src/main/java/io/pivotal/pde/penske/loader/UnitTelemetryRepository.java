 package io.pivotal.pde.penske.loader;

import java.util.List;

import org.springframework.data.gemfire.repository.GemfireRepository;
import org.springframework.data.gemfire.repository.Query;

import io.pivotal.pde.penske.unittelemetry.aggregator.model.UnitTelemetry;

public interface UnitTelemetryRepository extends GemfireRepository<UnitTelemetry, String> {

	@Query("select distinct vin from /UnitTelemetry")
	List<String> findAllVin();
	
	@Query("select distinct id from /UnitTelemetry")
	List<String> findAllIds();
	
	@Query("select distinct * from /UnitTelemetry where vin=$1 order by capture_datetime desc")
	List<UnitTelemetry> findByVin(String vin);
}
