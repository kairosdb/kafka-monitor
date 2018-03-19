package org.kairosdb.kafka.monitor;

import com.google.common.collect.EvictingQueue;

/**
 Used to calculate the time to process the lag
 */
public class ProcessRate
{
	private final EvictingQueue<Double> m_processRates;
	private final Object m_ratesLock = new Object();


	public ProcessRate(int trackingCount)
	{
		m_processRates = EvictingQueue.create(trackingCount);
	}


	public void addRate(double rate)
	{
		synchronized (m_ratesLock)
		{
			m_processRates.add(rate);
		}
	}


	/**
	 Returns the average rate
	 @return
	 */
	public double getCurrentRate()
	{
		double retRate;

		synchronized (m_ratesLock)
		{
			double rateSum = 0D;

			for (Double rate : m_processRates)
			{
				rateSum += rate.doubleValue();
			}

			if (m_processRates.size() == 0)
				retRate = 0D;
			else
				retRate = rateSum / (double) m_processRates.size();
		}

		return retRate;
	}
}
