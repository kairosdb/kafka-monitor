package org.kairosdb.kafka.monitor;

import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestGroupStats
{
	@Test
	public void test_initialOffsets()
	{
		GroupStats gs = new GroupStats("foo", "topic", 10);

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(3, 5, 1000);
		gs.offsetChange(4, 5, 1000);

		assertEquals(0.0, gs.getCurrentRate());
	}

	@Test
	public void test_updateOffsets()
	{
		Stopwatch rateTimer = mock(Stopwatch.class);
		when(rateTimer.start()).thenReturn(rateTimer);
		when(rateTimer.reset()).thenReturn(rateTimer);
		when(rateTimer.elapsed(TimeUnit.SECONDS)).thenReturn(1L);

		GroupStats gs = new GroupStats("foo", "topic", 10);

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(1, 15, 2000);
		gs.offsetChange(2, 15, 2000);

		assertEquals(0.01, gs.getCurrentRate());
	}

	@Test
	public void test_offsetChangeWithNoTimeChange()
	{
		GroupStats gs = new GroupStats("foo", "topic", 10);

		gs.offsetChange(1, 5, 1000);
		gs.offsetChange(2, 5, 1000);
		gs.offsetChange(1, 15, 1000);
		gs.offsetChange(2, 15, 1000);

		assertEquals(0D, gs.getCurrentRate());
	}
}
