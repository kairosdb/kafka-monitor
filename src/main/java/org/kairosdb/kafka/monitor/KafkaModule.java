package org.kairosdb.kafka.monitor;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;

import javax.inject.Named;
import java.util.Properties;

public class KafkaModule  extends AbstractModule
{

	@Override
	protected void configure()
	{
		bind(OffsetListenerService.class).in(Singleton.class);
		bind(MonitorConfig.class).in(Singleton.class);
		bind(UpdateTopicsJob.class).in(Singleton.class);
		bind(OffsetsTracker.class).in(Singleton.class);

		//bind(RawOffsetReader.class).in(Singleton.class);
		//bind(PartitionedOffsetReader.class).in(Singleton.class);

		/*
		 Bind object for default consumer config
		 bind separate services
		   one service for reading offsets and partitioning them
		   one service for reading partitioned offsets and reporting metrics
		   one service for reading ownership

		 bind singleton to keep track of global data
		 */
	}

	@Provides
	@Named("DefaultConfig")
	public Properties getDefaultConfig(MonitorConfig config)
	{
		Properties defaultConfig = new Properties();

		defaultConfig.put(ConsumerConfig.GROUP_ID_CONFIG, config.getApplicationId());
		defaultConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootStrapServers());
		defaultConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		defaultConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put("exclude.internal.topics", "false");
		defaultConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
		defaultConfig.put(ProducerConfig.RETRIES_CONFIG, 10);
		defaultConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30*1000);
		//If we never commit offsets we will always get the latest or earliest depending on topic
		defaultConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		return defaultConfig;
	}
}
