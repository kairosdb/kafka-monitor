package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class PartitionedOffsetReader extends TopicReader
{
	private static final Logger logger = LoggerFactory.getLogger(PartitionedOffsetReader.class);

	private final OffsetsTracker m_offsetTracker;
	private final Properties m_consumerConfig;
	private final MonitorConfig m_monitorConfig;
	private final int m_instanceId;

	private KafkaConsumer<String, Offset> m_consumer;
	private KafkaProducer<String, String> m_producer;


	@Inject
	public PartitionedOffsetReader(@Named("DefaultConfig") Properties defaultConfig,
			MonitorConfig monitorConfig, OffsetsTracker offsetsTracker, int instanceId)
	{
		m_consumerConfig = (Properties) defaultConfig.clone();
		m_monitorConfig = monitorConfig;
		m_offsetTracker = offsetsTracker;
		m_instanceId = instanceId;
	}


	@Override
	protected void initializeConsumers()
	{
		m_consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		m_consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Offset.OffsetDeserializer.class);

		m_consumerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		m_consumerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		m_consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, m_monitorConfig.getClientId()+"_partitioned_"+m_instanceId); //cannot have the same app id
		m_consumer = new KafkaConsumer<>(m_consumerConfig);

		m_producer = new KafkaProducer<>(m_consumerConfig);

		m_consumer.subscribe(Collections.singleton(m_monitorConfig.getOffsetsTopicName()));
		m_consumer.seekToEnd(m_consumer.assignment());
	}

	@Override
	protected void stopConsumers()
	{
		m_consumer.close();
		m_producer.close();
	}


	@Override
	protected void readTopic()
	{
		ConsumerRecords<String, Offset> records = m_consumer.poll(100);
		Set<String> ownedTopics = new HashSet<>();

		for (ConsumerRecord<String, Offset> record : records)
		{
			m_offsetTracker.updateOffset(record.value());

			ownedTopics.add(record.key());
		}

		for (String ownedTopic : ownedTopics)
		{
			m_producer.send(new ProducerRecord<String, String>(
					m_monitorConfig.getTopicOwnerTopicName(), ownedTopic, m_monitorConfig.getClientId()));
		}
	}
}

