package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import jnr.ffi.byref.ByteByReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class OffsetListenerService implements KairosDBService
{
	private final Publisher<DataPointEvent> m_publisher;
	private KafkaStreams m_offsetStream;

	@Inject
	public OffsetListenerService(FilterEventBus eventBus)
	{
		m_publisher = checkNotNull(eventBus).createPublisher(DataPointEvent.class);
	}


	@Override
	public void start() throws KairosDBException
	{
		final Properties streamConfig = new Properties();

		streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-monitor");
		streamConfig.put(StreamsConfig.CLIENT_ID_CONFIG, "client-01");
		streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "m0077509.lab.ppops.net:9092");
		streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, "stream_state");
		streamConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor");
		streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		streamConfig.put("exclude.internal.topics", "false");

		KStreamBuilder builder = new KStreamBuilder();
		builder.stream(Serdes.Bytes(), Serdes.Bytes(), "__consumer_offsets")
				.map(new KeyValueMapper<Bytes, Bytes, KeyValue<OffsetKey, OffsetValue>>()
				{
					@Override
					public KeyValue<OffsetKey, OffsetValue> apply(Bytes key, Bytes value)
					{
						ByteBuffer bb = ByteBuffer.wrap(key.get());
						int version = bb.getShort();
						int strSize = bb.getShort();
						byte[] groupName = new byte[strSize];
						bb.get(groupName);
						int partition = -1;
						byte[] topicName = "NULL".getBytes();
						if (bb.position() != bb.limit())
						{
							strSize = bb.getShort();
							topicName = new byte[strSize];
							bb.get(topicName);
							partition = bb.getInt();
						}

						OffsetKey offsetKey = new OffsetKey(new String(groupName), new String(topicName), partition);


						bb = ByteBuffer.wrap(value.get());
						version = bb.getShort();

						long offset = bb.getLong();
						strSize = bb.getShort();
						String metadata = "NO_METADATA";
						if (strSize != 0)
						{
							byte[] metadataBytes = new byte[strSize];
							bb.get(metadataBytes);
							metadata = new String(metadataBytes);
						}
						long commitTimestamp = bb.getLong();

						long expireTimestamp = -1;
						if (version == 1)
							expireTimestamp = bb.getLong();

						OffsetValue offsetValue = new OffsetValue(offset, commitTimestamp, expireTimestamp);

						return new KeyValue<OffsetKey, OffsetValue>(offsetKey, offsetValue);
					}
				});



		//Swap out the context class loader, should be done in kairos before calling plugin
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

		m_offsetStream = new KafkaStreams(builder, streamConfig);

		m_offsetStream.start();

		//put it back
		Thread.currentThread().setContextClassLoader(contextClassLoader);
	}

	@Override
	public void stop()
	{
		m_offsetStream.close();
	}
}
