package org.kairosdb.kafka.monitor;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kairosdb.kafka.monitor.util.Stopwatch;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 Reads partitioned offsets by topic name and reports metrics on the offsets.
 */
public class PartitionedOffsetReader extends TopicReader
{
	private static final Logger logger = LoggerFactory.getLogger(PartitionedOffsetReader.class);
	private static final MonitorStats stats = MetricSourceManager.getSource(MonitorStats.class);

	private final OffsetsTracker m_offsetTracker;
	private final Properties m_consumerConfig;
	private final MonitorConfig m_monitorConfig;
	private final int m_instanceId;
	private final Properties m_producerConfig;
	private final long m_reportingDelayMillis;
	private final Set<String> m_ownedTopics = new HashSet<>();
	private Stopwatch m_ownershipDelay = Stopwatch.createStarted();

	private KafkaConsumer<String, Offset> m_consumer;
	private KafkaProducer<String, String> m_producer;


	@Inject
	public PartitionedOffsetReader(@Named("DefaultConfig") Properties defaultConfig,
			MonitorConfig monitorConfig, OffsetsTracker offsetsTracker, int instanceId)
	{
		super(monitorConfig.getDeadClientRestart());
		m_monitorConfig = monitorConfig;
		m_offsetTracker = offsetsTracker;
		m_instanceId = instanceId;
		m_reportingDelayMillis = m_monitorConfig.getOwnershipReportingDelay().toMillis();

		m_consumerConfig = (Properties) defaultConfig.clone();

		m_consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, m_monitorConfig.getClientId()+"_partitioned_"+m_instanceId); //cannot have the same app id
		m_consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		m_producerConfig = (Properties) m_consumerConfig.clone();

		m_consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		m_consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Offset.OffsetDeserializer.class);

		m_producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		m_producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	}


	@Override
	protected void initializeConsumers()
	{
		m_consumer = new KafkaConsumer<>(m_consumerConfig);
		m_producer = new KafkaProducer<>(m_producerConfig);

		m_consumer.subscribe(Collections.singleton(m_monitorConfig.getOffsetsTopicName()));
	}

	@Override
	protected void stopConsumers()
	{
		m_consumer.close();
		m_producer.close();
	}


	@Override
	protected int readTopic()
	{
		ConsumerRecords<String, Offset> records = m_consumer.poll(Duration.ofMillis(100));

		int count = records.count();

		stats.partitionedOffsetsRead().put(count);

		for (ConsumerRecord<String, Offset> record : records)
		{
			m_offsetTracker.updateOffset(record.value());

			m_ownedTopics.add(record.key());
		}

		if (m_ownershipDelay.elapsed(TimeUnit.MILLISECONDS) > m_reportingDelayMillis)
		{
			for (String ownedTopic : m_ownedTopics)
			{
				m_producer.send(new ProducerRecord<>(
						m_monitorConfig.getTopicOwnerTopicName(), ownedTopic, m_monitorConfig.getClientId()));
			}

			m_ownedTopics.clear();
			m_ownershipDelay.reset().start();
		}

		return count;
	}
}

