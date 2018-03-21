package org.kairosdb.kafka.monitor;

import org.junit.Test;
import org.kairosdb.kafka.monitor.util.Stopwatch;

import java.security.acl.Group;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestGroupStats
{
	private Stopwatch getStopwatch(int seconds)
	{
		Stopwatch rateTimer = mock(Stopwatch.class);
		when(rateTimer.start()).thenReturn(rateTimer);
		when(rateTimer.reset()).thenReturn(rateTimer);
		when(rateTimer.elapsed(TimeUnit.SECONDS)).thenReturn((long)seconds);

		return rateTimer;
	}

	@Test
	public void test_initialOffsets()
	{
		Stopwatch rateTimer = getStopwatch(10);

		GroupStats gs = new GroupStats("foo", "topic", 10);
		gs.setRateTimer(rateTimer);

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(3, 5, 1000);
		gs.offsetChange(4, 5, 1000);

		gs = gs.copyAndReset();

		assertEquals(0.0, gs.getCurrentRate());
	}

	@Test
	public void test_updateOffsets()
	{
		Stopwatch rateTimer = getStopwatch(1);

		GroupStats gs = new GroupStats("foo", "topic", 10);
		gs.setRateTimer(rateTimer);

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(1, 15, 2000);
		gs.offsetChange(2, 15, 2000);

		gs = gs.copyAndReset();

		assertEquals(20D, gs.getCurrentRate());
	}

	@Test
	public void test_offsetChangeWithNoTimeChange()
	{
		Stopwatch rateTimer = getStopwatch(1);
		GroupStats gs = new GroupStats("foo", "topic", 10);
		gs.setRateTimer(rateTimer);

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(1, 15, 1000);
		gs.offsetChange(2, 15, 1000);

		gs = gs.copyAndReset();

		assertEquals(0D, gs.getCurrentRate());
	}

	@Test
	public void test_proxyGroup_noSplit()
	{
		GroupStats gs = new GroupStats("MyAwesomeGroup", "topic", 10);

		assertEquals("MyAwesomeGroup", gs.getProxyGroup());
	}

	@Test
	public void test_proxyGroup_withGroup()
	{
		GroupStats gs = new GroupStats("MyAwesome__Group", "topic", 10);

		assertEquals("MyAwesome", gs.getProxyGroup());
	}

	@Test
	public void test_proxyGroup_multipleSplits()
	{
		GroupStats gs = new GroupStats("My__Awesome__Group", "topic", 10);

		assertEquals("My__Awesome", gs.getProxyGroup());
	}
}
