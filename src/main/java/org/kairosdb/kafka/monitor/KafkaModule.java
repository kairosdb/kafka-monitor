package org.kairosdb.kafka.monitor;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.kairosdb.kafka.monitor.util.ConfigurationTypeListener;

import javax.inject.Named;
import java.util.Map;
import java.util.Properties;

public class KafkaModule  extends AbstractModule
{

	@Override
	protected void configure()
	{
		bindListener(Matchers.any(), new ConfigurationTypeListener());

		bind(OffsetListenerService.class).in(Singleton.class);
		bind(MonitorConfig.class).in(Singleton.class);
		bind(UpdateTopicsJob.class).in(Singleton.class);
		bind(OffsetsTracker.class).in(Singleton.class);
		bind(MetricsTrigger.class).in(Singleton.class);
	}

	@Provides
	@Named("DefaultConfig")
	public Properties getDefaultConfig(MonitorConfig monitorConfig, Config config)
	{
		Properties defaultConfig = new Properties();

		defaultConfig.put(ConsumerConfig.GROUP_ID_CONFIG, monitorConfig.getApplicationId());
		defaultConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, monitorConfig.getBootStrapServers());
		defaultConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		defaultConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
		defaultConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
		defaultConfig.put(ProducerConfig.RETRIES_CONFIG, 10);
		defaultConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30*1000);
		//If we never commit offsets we will always get the latest or  earliest depending on topic and that's ok
		defaultConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		Config clientConfig = config.getConfig("topic_monitor.client");

		for (Map.Entry<String, ConfigValue> configEntry : clientConfig.entrySet())
		{
			defaultConfig.put(configEntry.getKey(), configEntry.getValue().unwrapped().toString());
		}

		return defaultConfig;
	}
}
