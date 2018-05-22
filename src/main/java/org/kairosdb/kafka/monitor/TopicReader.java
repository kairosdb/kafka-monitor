package org.kairosdb.kafka.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class TopicReader implements Runnable
{
	private static final Logger logger = LoggerFactory.getLogger(TopicReader.class);
	private volatile boolean m_keepRunning = true;
	private CountDownLatch m_finishLatch;


	protected abstract void initializeConsumers();

	public void startReader()
	{
		initializeConsumers();
		m_finishLatch = new CountDownLatch(1);

		Executors.newSingleThreadExecutor().submit(this);
	}

	protected abstract void stopConsumers();

	public void stopReader()
	{
		m_keepRunning = false;

		try
		{
			m_finishLatch.await(30, TimeUnit.SECONDS);
		}
		catch (InterruptedException e)
		{
			Thread.interrupted();
			logger.warn("Stop thread interrupted", e);
		}

		try
		{
			stopConsumers();
		}
		catch (Exception e)
		{
			logger.error("Error stopping consumer", e);
		}
	}

	protected abstract void readTopic();

	@Override
	public void run()
	{
		while (m_keepRunning)
		{
			try
			{
				readTopic();
			}
			catch (Exception e)
			{
				logger.error("Error reading events", e);
				try
				{
					//Wait 5 sec so we don't flood logs
					Thread.sleep(5000);
				}
				catch (InterruptedException e1)
				{
					Thread.interrupted();
				}
			}
		}

		m_finishLatch.countDown();
	}
}
