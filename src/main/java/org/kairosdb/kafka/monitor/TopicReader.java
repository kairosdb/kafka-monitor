package org.kairosdb.kafka.monitor;

import org.kairosdb.kafka.monitor.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class TopicReader implements Runnable
{
	private static final Logger logger = LoggerFactory.getLogger(TopicReader.class);
	private final Duration m_deadClientRestart;
	private volatile boolean m_keepRunning = true;
	private CountDownLatch m_finishLatch;
	private final Controller m_controller;

	protected TopicReader(Duration deadClientRestart)
	{
		m_controller = new Controller();
		m_deadClientRestart = deadClientRestart;
	}

	private void internalStart()
	{
		m_keepRunning = true;
		initializeConsumers();
		m_finishLatch = new CountDownLatch(1);

		Thread readerThread = new Thread(this);
		readerThread.start();
	}

	private void internalStop()
	{
		m_keepRunning = false;

		try
		{
			logger.info("wait for read to finish");
			m_finishLatch.await(30, TimeUnit.SECONDS);
		}
		catch (InterruptedException e)
		{
			Thread.interrupted();
			logger.warn("Stop thread interrupted", e);
		}

		try
		{
			logger.info("calling stop consumers");
			stopConsumers();
		}
		catch (Exception e)
		{
			logger.error("Error stopping consumer", e);
		}
	}

	protected abstract void initializeConsumers();

	public Controller getController()
	{
		return m_controller;
	}

	protected abstract void stopConsumers();

	protected abstract int readTopic();

	@Override
	public void run()
	{
		Stopwatch responseTimer = Stopwatch.createStarted();
		int failureCount = 0;
		while (m_keepRunning)
		{
			try
			{
				int count = readTopic();
				if (count != 0)
				{
					responseTimer.reset().start();
				}
				failureCount = 0;
			}
			catch (Exception e)
			{
				failureCount ++;
				logger.error("Error reading events", e);
				try
				{
					if (failureCount > 10)
					{
						//restart client
						m_controller.restartReader(false);
					}
					else
					{
						//Wait 5 sec so we don't flood logs
						Thread.sleep(5000);
					}
				}
				catch (InterruptedException e1)
				{
					Thread.interrupted();
				}
			}

			//If we go so long without reading data from kafka, we restart the readers
			if (responseTimer.elapsed(TimeUnit.MILLISECONDS) > m_deadClientRestart.toMillis())
				m_controller.restartReader(false);
		}

		m_finishLatch.countDown();
	}

	public class Controller
	{
		public void startReader()
		{
			internalStart();
		}

		public void stopReader()
		{
			internalStop();
		}

		private void restart()
		{
			stopReader();
			try
			{
				Thread.sleep(5000);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			startReader();
		}

		public void restartReader(boolean wait)
		{
			m_keepRunning = false;
			if (wait)
				restart();
			else
			{
				new Thread(() -> restart()).start();
			}
		}
	}
}
