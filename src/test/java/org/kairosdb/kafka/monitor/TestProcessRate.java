package org.kairosdb.kafka.monitor;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestProcessRate
{
	@Test
	public void test_rateWhenNoneHaveBeenAdded()
	{
		ProcessRate rate = new ProcessRate(10);

		assertEquals(0D, rate.getCurrentRate());
	}

	@Test
	public void test_rateWithZeroValues()
	{
		ProcessRate rate = new ProcessRate(10);

		rate.addRate(0D);
		rate.addRate(0D);
		rate.addRate(0D);
		rate.addRate(0D);

		assertEquals(0D, rate.getCurrentRate());
	}

	@Test
	public void test_simpleAvg()
	{
		ProcessRate rate = new ProcessRate(10);

		rate.addRate(10D);
		rate.addRate(5D);
		rate.addRate(15D);
		rate.addRate(10D);

		assertEquals(10D, rate.getCurrentRate());
	}

	@Test
	public void test_decimalAvg()
	{
		ProcessRate rate = new ProcessRate(10);

		rate.addRate(10D);
		rate.addRate(5D);

		assertEquals(7.5, rate.getCurrentRate());
	}
}
