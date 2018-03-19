package org.kairosdb.kafka.monitor;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.kairosdb.kafka.monitor.OffsetStat.calculateDiff;

public class TestOffsetStat
{
	@Test
	public void test_calcuateDiffRollover()
	{
		long diff = calculateDiff(1, Long.MAX_VALUE -1);

		assertEquals(2, diff);
	}
}
