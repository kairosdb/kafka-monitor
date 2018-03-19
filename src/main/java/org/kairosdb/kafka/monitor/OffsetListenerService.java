package org.kairosdb.kafka.monitor;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import org.eclipse.jetty.util.ConcurrentHashSet;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.kafka.monitor.OffsetStat.calculateDiff;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

//todo Add configuration to expire tracked offsets

public class OffsetListenerService implements KairosDBService, KairosDBJob
{
	private static final Logger logger = LoggerFactory.getLogger(OffsetListenerService.class);

	private final Publisher<DataPointEvent> m_publisher;
	private final LongDataPointFactory m_dataPointFactory;
	private KafkaStreams m_offsetStream;
	private KafkaStreams m_topicOwnerStream;
	private Set<String> m_topics = new ConcurrentHashSet<>();
	private Map<Pair<String, String>, GroupStats> m_groupStatsMap = new ConcurrentHashMap<>();
	private KafkaConsumer<Bytes, Bytes> m_consumer;
	private Map<String, List<PartitionInfo>> m_kafkaTopics = new HashMap<>(); //periodically updated list of topics in kafka
	private final MonitorConfig m_monitorConfig;
	private final String m_clientId;
	private final ReentrantLock m_executeLock = new ReentrantLock();

	@Inject
	public OffsetListenerService(FilterEventBus eventBus,
			LongDataPointFactory dataPointFactory,
			MonitorConfig monitorConfig)
	{
		m_publisher = checkNotNull(eventBus).createPublisher(DataPointEvent.class);
		m_dataPointFactory = dataPointFactory;
		m_monitorConfig = monitorConfig;
		m_clientId = m_monitorConfig.getClientId();
	}

	//Called by external job to refresh our partition data
	//todo add external kairosDBJob that calls this every 10 min or so
	public void updateKafkaTopics()
	{
		Stopwatch timer = Stopwatch.createStarted();

		m_kafkaTopics = m_consumer.listTopics();

		//todo change to debug statement
		//dSystem.out.println("List topics: " + timer.stop().elapsed(TimeUnit.MILLISECONDS));
	}


	@Override
	public void start() throws KairosDBException
	{
		final Properties defaultConfig = new Properties();

		defaultConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, m_monitorConfig.getApplicationId());
		defaultConfig.put(StreamsConfig.CLIENT_ID_CONFIG, m_clientId);
		defaultConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, m_monitorConfig.getBootStrapServers());
		defaultConfig.put(StreamsConfig.STATE_DIR_CONFIG, m_monitorConfig.getStreamStateDirectory());
		defaultConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor");
		defaultConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		defaultConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Bytes().deserializer().getClass().getName());
		defaultConfig.put("exclude.internal.topics", "false");

		Properties offsetProperties = (Properties) defaultConfig.clone();
		Properties ownerProperties = (Properties) defaultConfig.clone();
		offsetProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

		m_consumer = new KafkaConsumer<Bytes, Bytes>(defaultConfig);

		//Stream to listen to offset changes
		KStreamBuilder offsetStreamBuilder = new KStreamBuilder();
		KStream<String, Offset> offsetStream = offsetStreamBuilder.stream(Serdes.Bytes(), Serdes.Bytes(), "__consumer_offsets")
				.filter(new Predicate<Bytes, Bytes>()
				{
					@Override
					public boolean test(Bytes key, Bytes value)
					{
						if (key != null && value != null)
						{
							//This code only handles versions 0 and 1 of the offsets
							//version 2 appears to mean something else
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
						//Map the offsets by topic for load balancing
						Offset offset = Offset.createFromBytes(key.get(), value.get());

						return new KeyValue<String, Offset>(offset.getTopic(), offset);
					}
				});

		if (m_monitorConfig.isExcludeMonitorOffsets())
		{
			final String applicationId = m_monitorConfig.getApplicationId();
			offsetStream = offsetStream.filter(new Predicate<String, Offset>()
			{
				@Override
				public boolean test(String key, Offset value)
				{
					return (!value.getGroup().startsWith(applicationId));
				}
			});
		}

		offsetStream.groupByKey(Serdes.String(), new Offset.OffsetSerde())
				.aggregate(new Initializer<String>()
				           {
					           @Override
					           public String apply()
					           {
						           return m_clientId;
					           }
				           },
						new Aggregator<String, Offset, String>()
						{
							@Override
							public String apply(String key, Offset value, String aggregate)
							{
								//Collect the data on our offsets and then notify others
								//we own this topic
								m_topics.add(value.getTopic());

								collectData(value);

								return m_clientId;
							}
						},
						Serdes.String(), "stream").to(Serdes.String(), Serdes.String(), m_monitorConfig.getTopicOwnerTopicName());


		ownerProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, m_monitorConfig.getApplicationId()+"_"+m_clientId); //cannot have the same app id
		KStreamBuilder topicOwnerBuilder = new KStreamBuilder();
		topicOwnerBuilder.stream(Serdes.String(), Serdes.String(), m_monitorConfig.getTopicOwnerTopicName())
				.foreach(new ForeachAction<String, String>()
				{
					@Override
					public void apply(String key, String value)
					{
						//System.out.println(value + " owns topic "+ key);
						if (!value.equals(m_clientId))
							m_topics.remove(key); //We do not own the topic anymore
					}
				});

		m_offsetStream = new KafkaStreams(offsetStreamBuilder, offsetProperties);
		m_offsetStream.start();

		m_topicOwnerStream = new KafkaStreams(topicOwnerBuilder, ownerProperties);
		m_topicOwnerStream.start();

		//put it back
		Thread.currentThread().setContextClassLoader(contextClassLoader);
	}


	/**
	 Collects and maintains a map of consumer offsets
	 @param value
	 */
	private void collectData(Offset value)
	{
		Pair<String, String> groupKey = Pair.of(value.getTopic(), value.getGroup());
		GroupStats groupStats = m_groupStatsMap.get(groupKey);

		if (groupStats == null)
		{
			groupStats = new GroupStats(value.getGroup(), value.getTopic(),
					m_monitorConfig.getRateTrackerSize());
			m_groupStatsMap.put(groupKey, groupStats);
		}

		groupStats.offsetChange(value.getPartition(), value.getOffset(), value.getCommitTime());
	}

	@Override
	public void stop()
	{
		m_offsetStream.close(30, TimeUnit.SECONDS);
		m_topicOwnerStream.close(30, TimeUnit.SECONDS);
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
		m_currentTopicOffsets.put(topic, ret);

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
		//System.out.println("Get latest topic offsets: " + timer.stop().elapsed(TimeUnit.MILLISECONDS));

		return ret;
	}



	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		//time to report metrics - round to nearest minute so in the edge case of multiple monitors
		//reporting the same metric they will just overwrite each other.
		long now = DateUtils.round(new Date(), Calendar.MINUTE).getTime();

		//If the previous run is still reporting we bail out
		if (!m_executeLock.tryLock())
			return;

		try
		{
			Stopwatch timer = Stopwatch.createStarted();

			//Copy of m_groupStatsMap to use while reporting stats
			Map<Pair<String, String>, GroupStats> currentStats = new HashMap<>();

			//grab a copy of all consumer offsets before processing
			for (GroupStats groupStats : m_groupStatsMap.values())
			{
				currentStats.put(Pair.of(groupStats.getTopic(), groupStats.getGroupName()), groupStats.copyAndReset());
			}


			Set<Map.Entry<Pair<String, String>, GroupStats>> entrySet = currentStats.entrySet();
			for (Map.Entry<Pair<String, String>, GroupStats> groupStatsEntry : entrySet)
			{
				if (!m_topics.contains(groupStatsEntry.getValue().getTopic()))
				{
					entrySet.remove(groupStatsEntry);
					continue; //We don't own this topic anymore
				}

				GroupStats groupStats = groupStatsEntry.getValue();

				Map<Integer, Long> latestOffsets = getLatestTopicOffsets(groupStats.getTopic());

				ImmutableSortedMap<String, String> groupTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
						.putAll(m_monitorConfig.getAdditionalTags())
						.put("group", groupStats.getGroupName())
						.put("topic", groupStats.getTopic()).build();

				//todo make it optional if we report our own offsets
				DataPointEvent event = new DataPointEvent(m_monitorConfig.getConsumerRateMetric(),
						groupTags,
						m_dataPointFactory.createDataPoint(now, groupStats.getConsumeCount()));
				m_publisher.post(event);

				long groupLag = 0;

				for (OffsetStat offsetStat : groupStats.getOffsetStats())
				{
					//todo document that offset age may be lost when adding new nodes to the monitor group

					ImmutableSortedMap<String, String> partitionTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
							.putAll(m_monitorConfig.getAdditionalTags())
							.put("group", groupStats.getGroupName())
							.put("topic", groupStats.getTopic())
							.put("partition", String.valueOf(offsetStat.getPartition())).build();

					DataPointEvent offsetAge = new DataPointEvent(m_monitorConfig.getOffsetAgeMetric(),
							partitionTags,
							m_dataPointFactory.createDataPoint(now, (now - offsetStat.getTimestamp())));
					m_publisher.post(offsetAge);

					Long latestOffset = latestOffsets.get(offsetStat.getPartition());
					if (latestOffset != null) //in case something goes bananas
					{
						long partitionLag = calculateDiff(latestOffset, offsetStat.getOffset());
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


				long msToProcess = (long)((double)groupLag / groupStats.getCurrentRate());
				DataPointEvent groupMsToProcessEvent = new DataPointEvent(m_monitorConfig.getGroupTimeToProcessMetric(),
						groupTags,
						m_dataPointFactory.createDataPoint(now, msToProcess));
				m_publisher.post(groupMsToProcessEvent);

				//todo set last offsets with current - put this in external object.
			}

			//iterate through our topics and report producer rate
			for (String topic : m_topics)
			{
				Map<Integer, Long> lastOffsets = m_lastTopicOffsets.get(topic);
				Map<Integer, Long> currentOffsets = m_currentTopicOffsets.get(topic);
				if (lastOffsets == null || currentOffsets == null)
					continue;  //no offsets, topic may be coming or going

				long offsetCount = 0L;
				for (Integer partition : currentOffsets.keySet())
				{
					offsetCount += calculateDiff(
							currentOffsets.get(partition),
							lastOffsets.get(partition));
				}

				ImmutableSortedMap<String, String> producerTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
						.putAll(m_monitorConfig.getAdditionalTags())
						.put("topic", topic).build();

				DataPointEvent producerRateEvent = new DataPointEvent(m_monitorConfig.getProducerRateMetric(),
						producerTags,
						m_dataPointFactory.createDataPoint(now, offsetCount));
				m_publisher.post(producerRateEvent);
			}


			//reset current offsets
			m_lastTopicOffsets = m_currentTopicOffsets;
			m_currentTopicOffsets = new HashMap<>();

			//Report how long it took to gather/report offsets
			ImmutableSortedMap<String, String> offsetTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
					.putAll(m_monitorConfig.getAdditionalTags())
					.put("host", m_clientId).build();

			DataPointEvent offsetTimeEvent = new DataPointEvent(m_monitorConfig.getOffsetGatherTimeMetric(),
					offsetTags,
					m_dataPointFactory.createDataPoint(now, timer.stop().elapsed(TimeUnit.MILLISECONDS)));
			m_publisher.post(offsetTimeEvent);

		}
		catch (Exception e)
		{
			logger.error("Error processing kafka stats", e);

			//Report failure metric
			ImmutableSortedMap<String, String> offsetTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
					.putAll(m_monitorConfig.getAdditionalTags())
					.put("host", m_clientId).build();

			DataPointEvent failureEvent = new DataPointEvent(m_monitorConfig.getGatherFailureMetric(),
					offsetTags,
					m_dataPointFactory.createDataPoint(now, 1));
			m_publisher.post(failureEvent);

			//Restart the client
			stop();
			try
			{
				start();
			}
			catch (KairosDBException e1)
			{
				logger.error("Failed to start monitor service", e1);
			}
		}
		finally
		{
			m_executeLock.unlock();
		}
	}
}
