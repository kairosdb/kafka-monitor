package org.kairosdb.kafka.monitor;

public class OffsetKey
{
	private final String m_group;
	private final String m_topic;
	private final int m_partition;

	public OffsetKey(String group, String topic, int partition)
	{
		m_group = group;
		m_topic = topic;
		m_partition = partition;
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
}
