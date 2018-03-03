package org.kairosdb.kafka.monitor;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

//todo make topics configurable
//todo make all configurations configurable

public class OffsetListenerService implements KairosDBService, KairosDBJob
{
	private static final Logger logger = LoggerFactory.getLogger(OffsetListenerService.class);

	private final Publisher<DataPointEvent> m_publisher;
	private final LongDataPointFactory m_dataPointFactory;
	private KafkaStreams m_offsetStream;
	private KafkaStreams m_topicOwnerStream;
	private Set<String> m_topics = new HashSet<String>();
	private Map<String, GroupStats> m_groupStatsMap = new ConcurrentHashMap<>();
	private KafkaConsumer<Bytes, Bytes> m_consumer;
	private Map<String, List<PartitionInfo>> m_kafkaTopics = new HashMap<>(); //periodically updated list of topics in kafka
	private final MonitorConfig m_monitorConfig;

	@Inject
	public OffsetListenerService(FilterEventBus eventBus,
			LongDataPointFactory dataPointFactory,
			MonitorConfig monitorConfig)
	{
		m_publisher = checkNotNull(eventBus).createPublisher(DataPointEvent.class);
		m_dataPointFactory = dataPointFactory;
		m_monitorConfig = monitorConfig;
	}

	//Called by external job to refresh our data
	//todo add external kairosDBJob that calls this every 10 min or so
	public void updateKafkaTopics()
	{
		Stopwatch timer = Stopwatch.createStarted();

		m_kafkaTopics = m_consumer.listTopics();

		//todo change to debug statement
		System.out.println("List topics: " + timer.stop().elapsed(TimeUnit.MILLISECONDS));
	}


	@Override
	public void start() throws KairosDBException
	{
		final Properties defaultConfig = new Properties();

		final String clientId = m_monitorConfig.getClientId();

		defaultConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, m_monitorConfig.getApplicationId());
		defaultConfig.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
		defaultConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, m_monitorConfig.getBootStrapServers());
		defaultConfig.put(StreamsConfig.STATE_DIR_CONFIG, m_monitorConfig.getStreamStateDirectory());
		defaultConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor");
		defaultConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		defaultConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put("exclude.internal.topics", "false");

		Properties offsetProperties = (Properties) defaultConfig.clone();
		offsetProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		//Swap out the context class loader, should be done in kairos before calling plugin
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

		m_consumer = new KafkaConsumer<Bytes, Bytes>(defaultConfig);

		KStreamBuilder offsetStreamBuilder = new KStreamBuilder();
		offsetStreamBuilder.stream(Serdes.Bytes(), Serdes.Bytes(), "__consumer_offsets")
				.filter(new Predicate<Bytes, Bytes>()
				{
					@Override
					public boolean test(Bytes key, Bytes value)
					{
						if (key != null && value != null)
						{
							ByteBuffer bbkey = ByteBuffer.wrap(key.get());
							if (bbkey.getShort() > 1)
							{
								logger.debug("Unknown key {}", key);
								logger.debug("Unknown value: {}", value);
								return false;
							}

							ByteBuffer bbvalue = ByteBuffer.wrap(value.get());
							if (bbvalue.getShort() > 1)
							{
								logger.debug("Unknown value: {}", value);
								return false;
							}

							return true;
						}
						else
							return false;
					}
				})
				.map(new KeyValueMapper<Bytes, Bytes, KeyValue<String, Offset>>()
				{
					@Override
					public KeyValue<String, Offset> apply(Bytes key, Bytes value)
					{
						Offset offset = Offset.createFromBytes(key.get(), value.get());

						return new KeyValue<String, Offset>(offset.getTopic(), offset);
					}
				}).groupByKey(Serdes.String(), new Offset.OffsetSerde())
				.aggregate(new Initializer<String>()
				           {
					           @Override
					           public String apply()
					           {
						           return clientId;
					           }
				           },
						new Aggregator<String, Offset, String>()
						{
							@Override
							public String apply(String key, Offset value, String aggregate)
							{
								//System.out.println(value);
								m_topics.add(value.getTopic());

								collectData(value);

								return clientId;
							}
						},
						Serdes.String(), "stream").to(Serdes.String(), Serdes.String(), m_monitorConfig.getTopicOwnerTopicName());


		KStreamBuilder topicOwnerBuilder = new KStreamBuilder();
		topicOwnerBuilder.stream(Serdes.String(), Serdes.String(), m_monitorConfig.getTopicOwnerTopicName())
				.foreach(new ForeachAction<String, String>()
				{
					@Override
					public void apply(String key, String value)
					{
						if (!value.equals(clientId))
							m_topics.remove(key);
					}
				});

		m_offsetStream = new KafkaStreams(offsetStreamBuilder, offsetProperties);
		m_offsetStream.start();

		m_topicOwnerStream = new KafkaStreams(topicOwnerBuilder, defaultConfig);
		m_topicOwnerStream.start();

		//put it back
		Thread.currentThread().setContextClassLoader(contextClassLoader);



	}


	private void collectData(Offset value)
	{
		String groupKey = value.getTopic()+value.getGroup();
		GroupStats groupStats = m_groupStatsMap.get(groupKey);

		if (groupStats == null)
		{
			groupStats = new GroupStats(value.getGroup(), value.getTopic());
			m_groupStatsMap.put(groupKey, groupStats);
		}

		groupStats.offsetChange(value.getPartition(), value.getOffset(), value.getCommitTime());
	}

	@Override
	public void stop()
	{
		m_offsetStream.close();
		m_topicOwnerStream.close();
	}

	@Override
	public Trigger getTrigger()
	{
		return (newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(simpleSchedule()
						.withIntervalInMinutes(1).repeatForever())
				.build());
	}

	@Override
	public void interrupt() { }

	//todo move this into a separate util class
	private Map<String, Map<Integer, Long>> m_lastTopicOffsets = new HashMap<>();
	private Map<String, Map<Integer, Long>> m_currentTopicOffsets = new HashMap<>();

	private Map<Integer, Long> getLatestTopicOffsets(String topic)
	{
		Stopwatch timer = Stopwatch.createStarted();
		Map<Integer, Long> ret = m_currentTopicOffsets.get(topic);

		if (ret != null)
			return ret;

		ret = new HashMap<>();

		List<PartitionInfo> partitionInfos = m_kafkaTopics.get(topic);

		if (partitionInfos == null)
			updateKafkaTopics();

		partitionInfos = m_kafkaTopics.get(topic);
		List<TopicPartition> partitions = new ArrayList<>();
		for (PartitionInfo partitionInfo : partitionInfos)
		{
			partitions.add(new TopicPartition(topic, partitionInfo.partition()));
		}

		Map<TopicPartition, Long> topicPartitionLongMap = m_consumer.endOffsets(partitions);

		for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap.entrySet())
		{
			ret.put(entry.getKey().partition(), entry.getValue());
		}

		//todo change to debug statement
		System.out.println("Get latest topic offsets: " + timer.stop().elapsed(TimeUnit.MILLISECONDS));

		return ret;
	}

	private long calculateLag(long consumerOffset, long head)
	{
		if (consumerOffset > head)
		{
			return (Long.MAX_VALUE - consumerOffset) + head;
		}
		else
			return head - consumerOffset;
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		try
		{
			long now = System.currentTimeMillis();
			Collection<GroupStats> values = m_groupStatsMap.values();
			for (GroupStats groupStats : values)
			{
				Map<Integer, Long> latestOffsets = getLatestTopicOffsets(groupStats.getTopic());

				ImmutableSortedMap<String, String> groupTags = ImmutableSortedMap.of("group", groupStats.getGroupName(), "topic", groupStats.getTopic());

				//todo make it optional if we report our own offsets
				DataPointEvent event = new DataPointEvent(m_monitorConfig.getConsumerRateMetric(),
						groupTags,
						m_dataPointFactory.createDataPoint(now, groupStats.getAndResetCounter()));
				m_publisher.post(event);

				long groupLag = 0;

				for (OffsetStat offsetStat : groupStats.getOffsetStats())
				{
					//todo document that offset age may be lost when adding new nodes to the monitor group
					ImmutableSortedMap<String, String> partitionTags = ImmutableSortedMap.of("group", groupStats.getGroupName(),
							"topic", groupStats.getTopic(),
							"partition", String.valueOf(offsetStat.getPartition()));
					DataPointEvent offsetAge = new DataPointEvent(m_monitorConfig.getOffsetAgeMetric(),
							partitionTags,
							m_dataPointFactory.createDataPoint(now, (now - offsetStat.getTimestamp())));
					m_publisher.post(offsetAge);

					Long latestOffset = latestOffsets.get(offsetStat.getPartition());
					if (latestOffset != null) //in case something goes bananas
					{
						long partitionLag = calculateLag(offsetStat.getOffset(), latestOffset);
						groupLag += partitionLag;

						DataPointEvent partitionLagEvent = new DataPointEvent(m_monitorConfig.getPartitionLagMetric(),
								partitionTags,
								m_dataPointFactory.createDataPoint(now, partitionLag));
						m_publisher.post(partitionLagEvent);
					}
				}

				DataPointEvent groupLagEvent = new DataPointEvent(m_monitorConfig.getGroupLagMetric(),
						groupTags,
						m_dataPointFactory.createDataPoint(now, groupLag));
				m_publisher.post(groupLagEvent);

				//todo report diff latestOffsets with previous gathered - ie producer rate

				//todo set last offsets with current - put this in external object.
			}


			//reset
			m_currentTopicOffsets.clear();

		}
		catch (Exception e)
		{
			logger.error("Error processing kafka stats", e);
		}
	}
}
