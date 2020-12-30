package app;

import io.jooby.Jooby;
import io.jooby.di.GuiceModule;
import org.kairosdb.kafka.monitor.KTMGuiceModule;
import org.kairosdb.kafka.monitor.KafkaModule;
import org.kairosdb.kafka.monitor.MetricsTrigger;
import org.kairosdb.kafka.monitor.OffsetListenerService;
import org.kairosdb.kafka.monitor.UpdateTopicsJob;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.MetricsContext;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import java.util.Collections;
import java.util.Properties;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class App extends Jooby implements JobFactory
{
	private Scheduler m_scheduler;

	private OffsetListenerService m_offsetListenerService;

	public App()
	{
		Properties props = new Properties();
		props.setProperty("org.quartz.threadPool.threadCount", "4");
		props.setProperty(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");
		MetricsContext metricsContext = MetricSourceManager.getMetricConfig().getContext();


		try
		{
			StdSchedulerFactory factory = new StdSchedulerFactory(props);
			m_scheduler = factory.getScheduler();
			m_scheduler.setJobFactory(this);

			install(new KTMGuiceModule(new KafkaModule()));
			get("/", ctx -> "Welcome to Jooby!");

			onStarting(() ->
			{
				metricsContext.registerTrigger("kafka-monitor-trigger", require(MetricsTrigger.class));
				metricsContext.addTriggerToPath("kafka-monitor-trigger", Collections.EMPTY_LIST);
				m_scheduler.start();

				m_offsetListenerService = require(OffsetListenerService.class);
				m_offsetListenerService.start();

				scheduleJob(m_offsetListenerService.getClass(), 1);

				scheduleJob(UpdateTopicsJob.class, 10);
			});


			onStop(() ->
			{
				m_scheduler.shutdown(true);

				if (m_offsetListenerService != null)
					m_offsetListenerService.stop();
			});

		}
		catch (SchedulerException e)
		{
			e.printStackTrace();
		}
	}

	private void scheduleJob(Class jobClass, int intervalMin) throws SchedulerException
	{
		JobDetail jobDetail = JobBuilder.newJob(jobClass)
				.withIdentity(jobClass.getName()).build();

		m_scheduler.scheduleJob(jobDetail, newTrigger()
				.withIdentity(jobClass.getSimpleName())
				.withSchedule(simpleSchedule()
						.withIntervalInMinutes(1).repeatForever())
				.build());
	}

	public static void main(final String[] args)
	{
		runApp(args, App::new);
	}

	@Override
	public Job newJob(TriggerFiredBundle triggerFiredBundle, Scheduler scheduler) throws SchedulerException
	{
		return require(triggerFiredBundle.getJobDetail().getJobClass());
	}
}
