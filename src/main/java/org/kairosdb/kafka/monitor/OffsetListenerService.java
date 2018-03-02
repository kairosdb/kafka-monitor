package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import jnr.ffi.byref.ByteByReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class OffsetListenerService implements KairosDBService
{
	private final Publisher<DataPointEvent> m_publisher;
	private KafkaStreams m_offsetStream;
	private Set<String> m_topics = new HashSet<String>();

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
								System.out.println("Key: "+key.toString());
								System.out.println("Value: "+value.toString());
								return false;
							}

							ByteBuffer bbvalue = ByteBuffer.wrap(value.get());
							if (bbvalue.getShort() > 1)
							{
								System.out.println("Value: "+value.toString());
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
				.aggregate(new Initializer<Offset>()
		           {
			           @Override
			           public Offset apply()
			           {
				           return new Offset();
			           }
		           },
				new Aggregator<String, Offset, Offset>()
				{
					@Override
					public Offset apply(String key, Offset value, Offset aggregate)
					{
						//System.out.println(value);
						m_topics.add(value.getTopic());

						value.

						return new Offset();
					}
				},
				new Offset.OffsetSerde(), "stream");



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
