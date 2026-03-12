package app;


import edu.umd.cs.findbugs.annotations.NonNull;
import io.jooby.Jooby;
import io.jooby.Server;
import io.jooby.ServerOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

public class NullServer extends Server.Base
{
	private List<Jooby> m_applications;
	private ServerOptions m_options = new ServerOptions();

	@NonNull
	@Override
	public Server setOptions(@NonNull ServerOptions options)
	{
		m_options = options;
		return this;
	}

	@Override
	protected ServerOptions defaultOptions()
	{
		return m_options;
	}

	@NonNull
	@Override
	public String getName()
	{
		return "NullServer";
	}


	@NonNull
	@Override
	public Server start(@NonNull Jooby... application)
	{
		m_applications = Arrays.asList(application);
		addShutdownHook();
		fireStart(m_applications, Executors.newSingleThreadExecutor());
		fireReady(m_applications);
		return this;
	}

	@NonNull
	@Override
	public Server stop()
	{
		fireStop(m_applications);
		return this;
	}
}
