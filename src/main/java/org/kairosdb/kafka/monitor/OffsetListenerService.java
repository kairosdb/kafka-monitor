package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;

import static com.google.common.base.Preconditions.checkNotNull;

public class OffsetListenerService implements KairosDBService
{
	private final Publisher<DataPointEvent> m_publisher;

	@Inject
	public OffsetListenerService(FilterEventBus eventBus)
	{
		m_publisher = checkNotNull(eventBus).createPublisher(DataPointEvent.class);
	}


	@Override
	public void start() throws KairosDBException
	{
		
	}

	@Override
	public void stop()
	{

	}
}
