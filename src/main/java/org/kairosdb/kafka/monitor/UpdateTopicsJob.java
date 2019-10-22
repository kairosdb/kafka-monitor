package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;


public class UpdateTopicsJob implements InterruptableJob
{
	private final OffsetListenerService m_offsetService;

	@Inject
	public UpdateTopicsJob(OffsetListenerService offsetService)
	{
		m_offsetService = offsetService;
	}

	public void interrupt()
	{

	}

	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		m_offsetService.updateKafkaTopics();
	}
}
