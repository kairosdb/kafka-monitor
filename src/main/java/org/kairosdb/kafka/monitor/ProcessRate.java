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

			//skip rates that are zero, situation may be that no data was received
			int usableRates = 0;
			for (Double rate : m_processRates)
			{
				if (rate.doubleValue() != 0.0)
				{
					usableRates ++;
					rateSum += rate.doubleValue();
				}

			}

			if (usableRates == 0)
				retRate = 0D;
			else
				retRate = rateSum / (double) usableRates;
		}

		return retRate;
	}
}
