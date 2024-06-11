package app;


import edu.umd.cs.findbugs.annotations.NonNull;
import io.jooby.Jooby;
import io.jooby.Server;
import io.jooby.ServerOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NullServer extends Server.Base
{
	private List<Jooby> m_applications = new ArrayList<>();

	@NonNull
	@Override
	public Server setOptions(@NonNull ServerOptions options)
	{
		return this;
	}

	@NonNull
	@Override
	public String getName()
	{
		return "NullServer";
	}

	@NonNull
	@Override
	public ServerOptions getOptions()
	{
		return new ServerOptions();
	}

	@NonNull
	@Override
	public Server start(@NonNull Jooby application)
	{
		m_applications.add(application);
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
