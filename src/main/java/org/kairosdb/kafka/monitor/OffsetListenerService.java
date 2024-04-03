package org.kairosdb.kafka.monitor;

import com.google.common.base.Stopwatch;
import jakarta.inject.Named;
import jakarta.inject.Inject;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.kairosdb.kafka.monitor.OffsetStat.calculateDiff;


public class OffsetListenerService implements InterruptableJob
{
	private static final Logger logger = LoggerFactory.getLogger(OffsetListenerService.class);
	private static final ConsumerStats consumerStats = MetricSourceManager.getSource(ConsumerStats.class);
	public static final String OFFSET_TOPIC = "__consumer_offsets";
	private final MetricsTrigger m_metricsTrigger;

	private KafkaConsumer<Bytes, Bytes> m_listTopicConsumer;
	private KafkaConsumer<Bytes, Bytes> m_endOffsetsConsumer;
	private final MonitorConfig m_monitorConfig;
	private final ReentrantLock m_executeLock = new ReentrantLock();

	private final OffsetsTracker m_offsetsTracker;
	private final Properties m_defaultConfig;

	private RawOffsetReader m_rawOffsetReader;
	private PartitionedOffsetReader m_partitionedOffsetReader;
	private OwnerReader m_ownerReader;

	private ClassLoader m_kafkaClassLoader; //used when loading kafka clients

	@Inject
	public OffsetListenerService(
			MonitorConfig monitorConfig, OffsetsTracker offsetsTracker,
			@Named("DefaultConfig")Properties defaultConfig,
			MetricsTrigger metricsTrigger)
	{
		m_monitorConfig = monitorConfig;

		m_offsetsTracker = offsetsTracker;
		m_defaultConfig = defaultConfig;
		m_metricsTrigger = metricsTrigger;
	}

	//Called by external job to refresh our partition data
	public void updateKafkaTopics()
	{
		Stopwatch timer = Stopwatch.createStarted();

		if (m_listTopicConsumer != null)  //may not have been initialized when this is called the first time
			m_offsetsTracker.updateTopics(m_listTopicConsumer.listTopics());

		logger.info("List topics: " + timer.stop().elapsed(TimeUnit.MILLISECONDS));
	}


	public void start()
	{
		m_rawOffsetReader = new RawOffsetReader(m_defaultConfig, m_monitorConfig, 0);
		m_partitionedOffsetReader = new PartitionedOffsetReader(m_defaultConfig, m_monitorConfig, m_offsetsTracker, 0);
		m_ownerReader = new OwnerReader(m_defaultConfig, m_monitorConfig, m_offsetsTracker, 0);


		m_listTopicConsumer = new KafkaConsumer<Bytes, Bytes>(m_defaultConfig);

		m_endOffsetsConsumer = new KafkaConsumer<Bytes, Bytes>(m_defaultConfig);

		updateKafkaTopics();

		startConsumers();
	}

	private void startConsumers()
	{
		try
		{
			m_rawOffsetReader.getController().startReader();
			m_partitionedOffsetReader.getController().startReader();
			m_ownerReader.getController().startReader();
		}
		catch (Exception e)
		{
			logger.error("Unable to start kafka consumers", e);

			m_rawOffsetReader.getController().stopReader();
			m_partitionedOffsetReader.getController().stopReader();
			m_ownerReader.getController().stopReader();
		}
	}

	private void stopConsumers()
	{
		m_rawOffsetReader.getController().stopReader();
		m_partitionedOffsetReader.getController().stopReader();
		m_ownerReader.getController().stopReader();
	}

	private void restartConsumers()
	{
		m_rawOffsetReader.getController().restartReader(true);
		m_partitionedOffsetReader.getController().restartReader(true);
		m_ownerReader.getController().restartReader(true);
	}


	public void stop()
	{
		stopConsumers();
		m_listTopicConsumer.close();
		m_endOffsetsConsumer.close();
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

		logger.debug("Get latest topic offsets time: {}", timer.stop().elapsed(TimeUnit.MILLISECONDS));

		return ret;
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
					consumerStats.gatherFailure().put(1);
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

				//Report consumer rate
				consumerStats.consumeCount(groupStats.getGroupName(), groupStats.getProxyGroup(), groupStats.getTopic()).put(groupStats.getConsumeCount());

				long groupLag = 0;

				for (OffsetStat offsetStat : groupStats.getOffsetStats())
				{
					//todo document that offset age may be lost when adding new nodes to the monitor group

					//Report offset age
					long offsetAge = now - offsetStat.getCommitTime();
					consumerStats.offsetAge(groupStats.getGroupName(), groupStats.getProxyGroup(), groupStats.getTopic(),
							String.valueOf(offsetStat.getPartition())).put(Duration.ofMillis(offsetAge));

					Long latestOffset = latestOffsets != null ? latestOffsets.get(offsetStat.getPartition()) : null;
					if (latestOffset != null) //in case something goes bananas
					{
						long partitionLag = calculateDiff(latestOffset, offsetStat.getOffset());

						//Check for stale partitions that have not been read from
						if (partitionLag != 0 && offsetAge > m_monitorConfig.getStalePartitionAge().toMillis())
							consumerStats.stalePartitions(groupStats.getGroupName(), groupStats.getProxyGroup(), groupStats.getTopic()).put(Duration.ofMillis(offsetAge));

						groupLag += partitionLag;

						//Report partition lag
						consumerStats.partitionLag(groupStats.getGroupName(), groupStats.getProxyGroup(), groupStats.getTopic(),
								String.valueOf(offsetStat.getPartition())).put(partitionLag);
					}
				}

				//Report group lag
				consumerStats.groupLag(groupStats.getGroupName(), groupStats.getProxyGroup(), groupStats.getTopic()).put(groupLag);

				//Report time to process lag
				long secToProcess = 0;
				if (groupStats.getCurrentRate() != 0.0)
					secToProcess = (long)((double)groupLag / groupStats.getCurrentRate());

				consumerStats.groupTimeToProcess(groupStats.getGroupName(), groupStats.getProxyGroup(), groupStats.getTopic()).put(Duration.ofSeconds(secToProcess));
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

				consumerStats.produceCount(topic).put(offsetCount);
			}


			//reset current offsets
			m_lastTopicOffsets = m_currentTopicOffsets;
			m_currentTopicOffsets = new HashMap<>();

			//Report how long it took to gather/report offsets
			consumerStats.offsetGatherTime().put(Duration.ofMillis(timer.stop().elapsed(TimeUnit.MILLISECONDS)));
		}
		catch (Exception e)
		{
			logger.error("Error processing kafka stats", e);

			//Report failure metric
			consumerStats.gatherFailure().put(1);

			//todo this restart isn't working, system is wonky afterwords
			//Restart the client
			restartConsumers();
		}
		finally
		{
			m_executeLock.unlock();
		}

		m_metricsTrigger.reportMetrics();
	}
}
