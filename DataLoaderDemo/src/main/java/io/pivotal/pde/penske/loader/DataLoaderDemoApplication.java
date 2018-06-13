package io.pivotal.pde.penske.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.gemfire.repository.config.EnableGemfireRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = { "io.pivotal.pde.penske" })
@ImportResource("classpath:gemfire-cache.xml")
@EnableGemfireRepositories
@EnableScheduling
public class DataLoaderDemoApplication {
	private static final Log log = LogFactory.getLog(DataLoaderDemoApplication.class.getName());

	public static void main(String[] args) {
		SpringApplication.run(DataLoaderDemoApplication.class, args);
	}

	
}
