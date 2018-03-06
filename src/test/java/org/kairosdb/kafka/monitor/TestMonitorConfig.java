package org.kairosdb.kafka.monitor;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestMonitorConfig
{
	@Test
	public void test_additionalTags()
	{
		MonitorConfig config = new MonitorConfig();

		config.setAdditionalTags("tag1=foo");

		assertEquals(ImmutableMap.of("tag1", "foo"),
				config.getAdditionalTags());


		config.setAdditionalTags("tag1=foo;tag2=bar");

		assertEquals(ImmutableMap.of("tag1", "foo", "tag2", "bar"),
				config.getAdditionalTags());


		config.setAdditionalTags("");

		assertEquals(ImmutableMap.of(),
				config.getAdditionalTags());


		config.setAdditionalTags("tag1=");

		assertEquals(ImmutableMap.of("tag1", ""),
				config.getAdditionalTags());

	}

	@Test(expected = java.lang.IllegalArgumentException.class)
	public void test_badAdditionalTags()
	{
		MonitorConfig config = new MonitorConfig();

		config.setAdditionalTags("tag1");
	}
}
