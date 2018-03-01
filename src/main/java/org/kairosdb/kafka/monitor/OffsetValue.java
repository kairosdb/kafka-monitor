package org.kairosdb.kafka.monitor;

public class OffsetValue
{
	private final long m_offset;
	private final long m_commitTime;
	private final long m_expireTime;

	public OffsetValue(long offset, long commitTime, long expireTime)
	{
		m_offset = offset;
		m_commitTime = commitTime;
		m_expireTime = expireTime;
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
}
