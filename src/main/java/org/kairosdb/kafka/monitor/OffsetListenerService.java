package org.kairosdb.kafka.monitor;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
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

import javax.inject.Named;
import java.util.*;
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
	public static final String OFFSET_TOPIC = "__consumer_offsets";

	private final Publisher<DataPointEvent> m_publisher;
	private final LongDataPointFactory m_dataPointFactory;

	private KafkaConsumer<Bytes, Bytes> m_listTopicConsumer;
	private KafkaConsumer<Bytes, Bytes> m_endOffsetsConsumer;
	private final MonitorConfig m_monitorConfig;
	private final String m_clientId;
	private final ReentrantLock m_executeLock = new ReentrantLock();

	private final OffsetsTracker m_offsetsTracker;
	private final Properties m_defaultConfig;

	private RawOffsetReader m_rawOffsetReader;
	private PartitionedOffsetReader m_partitionedOffsetReader;
	private OwnerReader m_ownerReader;

	private long m_runCounter = 0; //Counts how many times metrics have been reported, used to now when to update topics

	private ClassLoader m_kafkaClassLoader; //used when loading kafka clients

	@Inject
	public OffsetListenerService(FilterEventBus eventBus,
			LongDataPointFactory dataPointFactory,
			MonitorConfig monitorConfig, OffsetsTracker offsetsTracker,
			@Named("DefaultConfig")Properties defaultConfig)
	{
		m_publisher = checkNotNull(eventBus).createPublisher(DataPointEvent.class);
		m_dataPointFactory = dataPointFactory;
		m_monitorConfig = monitorConfig;
		m_clientId = m_monitorConfig.getClientId();

		m_offsetsTracker = offsetsTracker;
		m_defaultConfig = defaultConfig;
	}

	//Called by external job to refresh our partition data
	public void updateKafkaTopics()
	{
		Stopwatch timer = Stopwatch.createStarted();

		if (m_listTopicConsumer != null)  //may not have been initialized when this is called the first time
			m_offsetsTracker.updateTopics(m_listTopicConsumer.listTopics());

		logger.info("List topics: " + timer.stop().elapsed(TimeUnit.MILLISECONDS));
	}


	@Override
	public void start() throws KairosDBException
	{

		//Kafka uses the thread context loader to load stuff.  We have to swap
		//it with the one that loaded this class as Kairos loaded this plugin
		//in a separate class loader.
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		m_kafkaClassLoader = this.getClass().getClassLoader();
		Thread.currentThread().setContextClassLoader(m_kafkaClassLoader);

		m_rawOffsetReader = new RawOffsetReader(m_defaultConfig, m_monitorConfig, 0);
		m_partitionedOffsetReader = new PartitionedOffsetReader(m_defaultConfig, m_monitorConfig, m_offsetsTracker, 0);
		m_ownerReader = new OwnerReader(m_defaultConfig, m_monitorConfig, m_offsetsTracker, 0);


		m_listTopicConsumer = new KafkaConsumer<Bytes, Bytes>(m_defaultConfig);

		m_endOffsetsConsumer = new KafkaConsumer<Bytes, Bytes>(m_defaultConfig);

		updateKafkaTopics();
		//resetOffsets();

		try
		{
			startConsumers();
		}
		finally
		{
			//put it back
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	private void startConsumers()
	{
		try
		{
			m_rawOffsetReader.startReader();
			m_partitionedOffsetReader.startReader();
			m_ownerReader.startReader();
		}
		catch (Exception e)
		{
			logger.error("Unable to start kafka consumers", e);

			m_rawOffsetReader.stopReader();
			m_partitionedOffsetReader.stopReader();
			m_ownerReader.stopReader();
		}
	}

	private void stopConsumers()
	{
		m_rawOffsetReader.stopReader();
		m_partitionedOffsetReader.stopReader();
		m_ownerReader.stopReader();
	}



	@Override
	public void stop()
	{
		stopConsumers();
		m_listTopicConsumer.close();
		m_endOffsetsConsumer.close();
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

		List<PartitionInfo> partitionInfos = m_offsetsTracker.getPartitionInfo(topic);

		if (partitionInfos == null)
		{
			updateKafkaTopics();
			partitionInfos = m_offsetsTracker.getPartitionInfo(topic);
		}


		List<TopicPartition> partitions = new ArrayList<>();
		for (PartitionInfo partitionInfo : partitionInfos)
		{
			partitions.add(new TopicPartition(topic, partitionInfo.partition()));
		}

		Map<TopicPartition, Long> topicPartitionLongMap = m_endOffsetsConsumer.endOffsets(partitions);

		for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap.entrySet())
		{
			if (entry.getValue() != null)
				ret.put(entry.getKey().partition(), entry.getValue());
		}

		//todo change to debug statement
		//System.out.println("Get latest topic offsets: " + timer.stop().elapsed(TimeUnit.MILLISECONDS));

		return ret;
	}

	private void postEvent(DataPointEvent event)
	{

		//Metrics are kinda wonky the first time through so we skip those.
		if (m_runCounter != 0)
			m_publisher.post(event);
	}


	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		//time to report metrics - round to nearest minute so in the edge case of multiple monitors
		//reporting the same metric they will just overwrite each other.
		long now = DateUtils.truncate(new Date(), Calendar.MINUTE).getTime();

		//If the previous run is still reporting we bail out
		if (!m_executeLock.tryLock())
			return;

		try
		{
			Stopwatch timer = Stopwatch.createStarted();

			for (GroupStats groupStats : m_offsetsTracker.copyOfCurrentStats())
			{
				Map<Integer, Long> latestOffsets = null;

				try
				{
					latestOffsets = getLatestTopicOffsets(groupStats.getTopic());
				}
				catch (Exception e)
				{
					logger.error("Error reading kafka offsets for topic: "+groupStats.getTopic(), e);

					//Report failure metric
					ImmutableSortedMap<String, String> offsetTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
							.putAll(m_monitorConfig.getAdditionalTags())
							.put("host", m_clientId).build();

					DataPointEvent failureEvent = new DataPointEvent(m_monitorConfig.getGatherFailureMetric(),
							offsetTags,
							m_dataPointFactory.createDataPoint(now, 1));
					postEvent(failureEvent);
					try
					{
						m_endOffsetsConsumer.close();
					}
					catch (Exception e1)
					{
						logger.error("Failed to close latest offset consumer", e1);
					}

					//Have to swap out the class loader when creating new consumers
					ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
					Thread.currentThread().setContextClassLoader(m_kafkaClassLoader);
					try
					{
						m_endOffsetsConsumer = new KafkaConsumer<Bytes, Bytes>(m_defaultConfig);
					}
					finally
					{
						//put it back
						Thread.currentThread().setContextClassLoader(contextClassLoader);
					}
				}

				ImmutableSortedMap<String, String> groupTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
						.putAll(m_monitorConfig.getAdditionalTags())
						.put("group", groupStats.getGroupName())
						.put("proxy_group", groupStats.getProxyGroup())
						.put("topic", groupStats.getTopic()).build();

				//Report consumer rate
				DataPointEvent event = new DataPointEvent(m_monitorConfig.getConsumerRateMetric(),
						groupTags,
						m_dataPointFactory.createDataPoint(now, groupStats.getConsumeCount()));
				postEvent(event);

				long groupLag = 0;

				for (OffsetStat offsetStat : groupStats.getOffsetStats())
				{
					//todo document that offset age may be lost when adding new nodes to the monitor group

					ImmutableSortedMap<String, String> partitionTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
							.putAll(m_monitorConfig.getAdditionalTags())
							.put("group", groupStats.getGroupName())
							.put("proxy_group", groupStats.getProxyGroup())
							.put("topic", groupStats.getTopic())
							.put("partition", String.valueOf(offsetStat.getPartition())).build();

					//Report offset age
					DataPointEvent offsetAge = new DataPointEvent(m_monitorConfig.getOffsetAgeMetric(),
							partitionTags,
							m_dataPointFactory.createDataPoint(now, (now - offsetStat.getCommitTime())));
					postEvent(offsetAge);

					Long latestOffset = latestOffsets != null ? latestOffsets.get(offsetStat.getPartition()) : null;
					if (latestOffset != null) //in case something goes bananas
					{
						long partitionLag = calculateDiff(latestOffset, offsetStat.getOffset());
						groupLag += partitionLag;

						//Report partition lag
						DataPointEvent partitionLagEvent = new DataPointEvent(m_monitorConfig.getPartitionLagMetric(),
								partitionTags,
								m_dataPointFactory.createDataPoint(now, partitionLag));
						postEvent(partitionLagEvent);
					}
				}

				//Report group lag
				DataPointEvent groupLagEvent = new DataPointEvent(m_monitorConfig.getGroupLagMetric(),
						groupTags,
						m_dataPointFactory.createDataPoint(now, groupLag));
				postEvent(groupLagEvent);


				//Report time to process lag
				long secToProcess = 0;
				if (groupStats.getCurrentRate() != 0.0)
					secToProcess = (long)((double)groupLag / groupStats.getCurrentRate());

				DataPointEvent groupMsToProcessEvent = new DataPointEvent(m_monitorConfig.getGroupTimeToProcessMetric(),
						groupTags,
						m_dataPointFactory.createDataPoint(now, secToProcess));
				postEvent(groupMsToProcessEvent);
			}


			//iterate through our topics and report producer rate
			for (String topic : m_offsetsTracker.getTopics())
			{
				Map<Integer, Long> lastOffsets = m_lastTopicOffsets.get(topic);
				Map<Integer, Long> currentOffsets = m_currentTopicOffsets.get(topic);
				if (lastOffsets == null || currentOffsets == null)
					continue;  //no offsets, topic may be coming or going

				long offsetCount = 0L;
				for (Integer partition : currentOffsets.keySet())
				{
					//When things go wrong in the kafka cluster these can be null
					if (partition != null && currentOffsets.get(partition) != null && lastOffsets.get(partition) != null)
					{
						offsetCount += calculateDiff(
								currentOffsets.get(partition),
								lastOffsets.get(partition));
					}
				}

				ImmutableSortedMap<String, String> producerTags = new ImmutableSortedMap.Builder<String, String>(Ordering.natural())
						.putAll(m_monitorConfig.getAdditionalTags())
						.put("topic", topic).build();

				DataPointEvent producerRateEvent = new DataPointEvent(m_monitorConfig.getProducerRateMetric(),
						producerTags,
						m_dataPointFactory.createDataPoint(now, offsetCount));
				postEvent(producerRateEvent);
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
			postEvent(offsetTimeEvent);

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
			postEvent(failureEvent);

			//todo this restart isn't working, system is wonky afterwords
			//Restart the client
			stopConsumers();
			startConsumers();
		}
		finally
		{
			m_executeLock.unlock();
		}

		m_runCounter ++;
		if (m_runCounter % 15 == 0) //update topics every 15 min
			updateKafkaTopics();
	}
}
