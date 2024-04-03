package org.kairosdb.kafka.monitor;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.jooby.Jooby;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

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
