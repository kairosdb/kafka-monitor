package org.kairosdb.kafka.monitor;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestGroupStats
{
	@Test
	public void test_initialOffsets()
	{
		GroupStats gs = new GroupStats("foo", "topic");

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(3, 5, 1000);
		gs.offsetChange(4, 5, 1000);

		assertEquals(0.0, gs.getCurrentRate());
	}

	@Test
	public void test_updateOffsets()
	{
		GroupStats gs = new GroupStats("foo", "topic");

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(1, 15, 2000);
		gs.offsetChange(2, 15, 2000);

		assertEquals(0.01, gs.getCurrentRate());
	}

	@Test
	public void test_offsetChangeWithNoTimeChange()
	{
		GroupStats gs = new GroupStats("foo", "topic");

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(1, 15, 1000);
		gs.offsetChange(2, 15, 1000);

		assertEquals(0D, gs.getCurrentRate());
	}
}
