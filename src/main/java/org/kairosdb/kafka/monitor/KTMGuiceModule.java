package org.kairosdb.kafka.monitor;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.util.Modules;
import io.jooby.Environment;
import io.jooby.Extension;
import io.jooby.Jooby;
import io.jooby.guice.GuiceModule;
import io.jooby.guice.JoobyModule;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 The purpose of this class is so we can override configuration with what is in
 DurationConfigModule
 */
public class KTMGuiceModule implements Extension
{
	private List<Module> modules = new ArrayList<>();


	public KTMGuiceModule(@Nonnull Module... modules)
	{
		Stream.of(modules).forEach(this.modules::add);
	}

	@Override public boolean lateinit() {
		return true;
	}

	@Override
	public void install(@Nonnull Jooby application)
	{
		Environment env = application.getEnvironment();
		modules.add(Modules.override(new JoobyModule(application)).with(new DurationConfigModule(application)));
		Stage stage = env.isActive("dev", "test") ? Stage.DEVELOPMENT : Stage.PRODUCTION;
		Injector injector = Guice.createInjector(stage, modules);

		GuiceModule guiceModule = new GuiceModule(injector);
		guiceModule.install(application);
	}
}
