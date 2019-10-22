package org.kairosdb.kafka.monitor;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestOffset
{
	@Test
	public void testSerde()
	{
		Offset offset = new Offset("group", "topic", 43, 12354L, 12L);

		Offset.OffsetSerde serde = new Offset.OffsetSerde();

		byte[] bytes = serde.serializer().serialize("sam", offset);

		Offset offset2 = serde.deserializer().deserialize("sam", bytes);

		assertEquals(offset, offset2);
	}

	@Test
	public void testSerde2()
	{
		Offset offset = new Offset();

		Offset.OffsetSerde serde = new Offset.OffsetSerde();

		byte[] bytes = serde.serializer().serialize("sam", offset);

		Offset offset2 = serde.deserializer().deserialize("sam", bytes);

		assertEquals(offset, offset2);
	}
}
