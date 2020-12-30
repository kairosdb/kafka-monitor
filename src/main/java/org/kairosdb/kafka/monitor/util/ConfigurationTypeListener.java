package org.kairosdb.kafka.monitor.util;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

public class ConfigurationTypeListener implements TypeListener
{
	@Override
	public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter)
	{
		System.out.println("Type: "+typeLiteral.getRawType().getName());
		//System.out.println("Encounter: "+typeEncounter);
	}
}
