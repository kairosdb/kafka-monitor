package org.kairosdb.kafka.monitor;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;

public class Offset
{
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	private String m_group;
	private String m_topic;
	private int m_partition;
	private long m_offset;
	private long m_commitTime;
	private long m_expireTime;

	public Offset()
	{
		m_group = "";
		m_topic = "";
		m_partition = -1;
		m_offset = -1;
		m_commitTime = -1;
		m_expireTime = -1;
	}

	public Offset(String group, String topic, int partition, long offset, long commitTime, long expireTime)
	{
		m_group = group;
		m_topic = topic;
		m_partition = partition;
		m_offset = offset;
		m_commitTime = commitTime;
		m_expireTime = expireTime;
	}

	public String getGroup()
	{
		return m_group;
	}

	public String getTopic()
	{
		return m_topic;
	}

	public int getPartition()
	{
		return m_partition;
	}

	public long getOffset()
	{
		return m_offset;
	}

	public long getCommitTime()
	{
		return m_commitTime;
	}

	public long getExpireTime()
	{
		return m_expireTime;
	}

	private static String readString(ByteBuffer bb)
	{
		return readString(bb, "");
	}

	private static String readString(ByteBuffer bb, String defaultString)
	{
		int strSize = bb.getShort();

		if (strSize != 0)
		{
			byte[] bytes = new byte[strSize];
			bb.get(bytes);

			return new String(bytes, UTF_8);
		}
		else
			return defaultString;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Offset offset = (Offset) o;
		return m_partition == offset.m_partition &&
				m_offset == offset.m_offset &&
				m_commitTime == offset.m_commitTime &&
				m_expireTime == offset.m_expireTime &&
				Objects.equals(m_group, offset.m_group) &&
				Objects.equals(m_topic, offset.m_topic);
	}

	@Override
	public int hashCode()
	{

		return Objects.hash(m_group, m_topic, m_partition, m_offset, m_commitTime, m_expireTime);
	}

	@Override
	public String toString()
	{
		return "Offset{" +
				"m_group='" + m_group + '\'' +
				", m_topic='" + m_topic + '\'' +
				", m_partition=" + m_partition +
				", m_offset=" + m_offset +
				", m_commitTime=" + m_commitTime +
				", m_expireTime=" + m_expireTime +
				'}';
	}

	public static Offset createFromBytes(byte[] key, byte[] value)
	{
		Offset offset = new Offset();

		//Read key
		ByteBuffer bb = ByteBuffer.wrap(key);
		int version = bb.getShort();

		offset.m_group = readString(bb);
		if (bb.position() != bb.limit())
		{
			offset.m_topic = readString(bb);
			offset.m_partition = bb.getInt();
		}

		//Read value
		bb = ByteBuffer.wrap(value);
		version = bb.getShort();

		offset.m_offset = bb.getLong();

		if (version == 3)
			bb.getInt(); //leader_epoch

		//We read the metadata but we don't do anything with it.
		readString(bb, "NO_METADATA");

		offset.m_commitTime =  bb.getLong();

		if (version == 1)
			offset.m_expireTime = bb.getLong();

		return offset;
	}

	private static OffsetSerializer serializer = new OffsetSerializer();
	private static OffsetDeserializer deserializer = new OffsetDeserializer();

	public static class OffsetSerde implements Serde<Offset>
	{
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) { }

		@Override
		public void close() { }

		@Override
		public Serializer<Offset> serializer()
		{
			return serializer;
		}

		@Override
		public Deserializer<Offset> deserializer()
		{
			return deserializer;
		}
	}

	public static class OffsetSerializer implements Serializer<Offset>
	{
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) { }

		@Override
		public byte[] serialize(String topic, Offset data)
		{
			byte[] groupBytes = data.m_group.getBytes(UTF_8);
			byte[] topicBytes = data.m_topic.getBytes(UTF_8);

			int size = 32 + groupBytes.length + topicBytes.length;
			ByteBuffer bb = ByteBuffer.allocate(size);

			bb.putShort((short) groupBytes.length);
			bb.put(groupBytes);
			bb.putShort((short) topicBytes.length);
			bb.put(topicBytes);

			bb.putInt(data.m_partition);
			bb.putLong(data.m_offset);
			bb.putLong(data.m_commitTime);
			bb.putLong(data.m_expireTime);

			return bb.array();
		}

		@Override
		public void close() { }
	}

	public static class OffsetDeserializer implements Deserializer<Offset>
	{
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) { }

		@Override
		public Offset deserialize(String topic, byte[] data)
		{
			Offset offset = new Offset();

			if (data != null)
			{
				ByteBuffer bb = ByteBuffer.wrap(data);

				offset.m_group = readString(bb);

				offset.m_topic = readString(bb);

				offset.m_partition = bb.getInt();
				offset.m_offset = bb.getLong();
				offset.m_commitTime = bb.getLong();
				offset.m_expireTime = bb.getLong();
			}

			return offset;
		}

		@Override
		public void close() { }
	}
}
