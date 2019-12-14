package org.kairosdb.kafka.monitor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OwnerReader extends TopicReader
{

	private final OffsetsTracker m_offsetsTracker;
	private final Properties m_consumerConfig;
	private final MonitorConfig m_monitorConfig;
	private final int m_instanceId;

	private KafkaConsumer<String, String> m_consumer;

	public OwnerReader(Properties defaultConfig, MonitorConfig monitorConfig,
			OffsetsTracker offsetsTracker, int instanceId)
	{
		super();
		m_offsetsTracker = offsetsTracker;
		m_consumerConfig = (Properties) defaultConfig.clone();
		m_monitorConfig = monitorConfig;
		m_instanceId = instanceId;
	}

	@Override
	protected void initializeConsumers()
	{
		m_consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		m_consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		m_consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, m_monitorConfig.getApplicationId()+"_"+
				m_monitorConfig.getClientId());
		m_consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, m_monitorConfig.getClientId()+"_"+m_instanceId);
		m_consumer = new KafkaConsumer<>(m_consumerConfig);

		m_consumer.subscribe(Collections.singleton(m_monitorConfig.getTopicOwnerTopicName()));
		m_consumer.seekToEnd(m_consumer.assignment());
	}

	@Override
	protected void stopConsumers()
	{
		m_consumer.close();
	}

	@Override
	protected int readTopic()
	{
		ConsumerRecords<String, String> records = m_consumer.poll(Duration.ofMillis(100));

		int count = records.count();
		for (ConsumerRecord<String, String> record : records)
		{
			if (!record.value().equals(m_monitorConfig.getClientId()))
				m_offsetsTracker.removeTrackedTopic(record.key()); //We do not own the topic anymore
		}

		return count;
	}
}
