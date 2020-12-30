package org.kairosdb.kafka.monitor;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Types;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import io.jooby.Jooby;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DurationConfigModule extends AbstractModule
{
	private final List<String> m_durationProps = Arrays.asList(
			"topic_monitor.tracker_retention",
			"topic_monitor.stale_partition_age",
			"topic_monitor.dead_client_restart");
	private final Jooby m_application;

	public DurationConfigModule(Jooby application)
	{
		m_application = application;
	}

	@Override
	public void configure()
	{
		Config config = m_application.getEnvironment().getConfig();

		for (String durationProp : m_durationProps)
		{
			if (config.hasPath(durationProp))
			{
				Duration duration = config.getDuration(durationProp);

				bind(Duration.class).annotatedWith(Names.named(durationProp)).toInstance(duration);
			}
		}
	}
}
