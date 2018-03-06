package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class UpdateTopicsJob implements KairosDBJob
{
	private final OffsetListenerService m_offsetService;

	@Inject
	public UpdateTopicsJob(OffsetListenerService offsetService)
	{
		m_offsetService = offsetService;
	}

	@Override
	public Trigger getTrigger()
	{
		return (newTrigger()
				.withIdentity(this.getClass().getSimpleName())
				.withSchedule(simpleSchedule()
						.withIntervalInMinutes(10).repeatForever())
				.build());
	}

	@Override
	public void interrupt()
	{

	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException
	{
		m_offsetService.updateKafkaTopics();
	}
}
