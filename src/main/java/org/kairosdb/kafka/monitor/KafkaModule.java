package org.kairosdb.kafka.monitor;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class KafkaModule  extends AbstractModule
{

	@Override
	protected void configure()
	{
		bind(OffsetListenerService.class).in(Singleton.class);
	}
}