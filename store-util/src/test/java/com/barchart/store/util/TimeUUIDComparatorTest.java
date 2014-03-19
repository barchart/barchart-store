package com.barchart.store.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Test;

public class TimeUUIDComparatorTest {

	@Test
	public void test() throws Exception {

		final TimeUUIDComparator cmp = new TimeUUIDComparator();

		final UUID first = UUIDUtil.timeUUID(1l);
		// Results in negative MSB when parsed into UUID
		final UUID second = UUIDUtil.timeUUID(100000000000000l);

		assertTrue(first.getMostSignificantBits() > 0);
		assertTrue(second.getMostSignificantBits() < 0);

		assertEquals(1, first.compareTo(second));
		assertEquals(-1, cmp.compare(first, second));

	}

}
