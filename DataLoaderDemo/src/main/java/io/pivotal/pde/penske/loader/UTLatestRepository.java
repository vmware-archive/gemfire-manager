package io.pivotal.pde.penske.loader;

import org.springframework.data.gemfire.repository.GemfireRepository;

/**
 * Created by 600158489 on 3/5/2018.
 */
public interface UTLatestRepository extends GemfireRepository<UTLatest, String> {
}
