package com.barchart.store.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ULongComparatorTest {

	@Test
	public void test() throws Exception {

		final ULongComparator cmp = new ULongComparator();

		// Basic checks
		assertEquals(0, cmp.compare(1l, 1l));
		assertEquals(1, cmp.compare(1l, 0l));
		assertEquals(1, cmp.compare(2l, 1l));
		assertEquals(-1, cmp.compare(0l, 1l));
		assertEquals(-1, cmp.compare(1l, 2l));

		// Signed/unsigned checks - negative is greater when unsigned due to MSB
		assertEquals(0, cmp.compare(-1l, -1l));
		assertEquals(-1, cmp.compare(1l, -1l));
		assertEquals(1, cmp.compare(-1l, 1l));

		// Outer limits
		assertEquals(0, cmp.compare(Long.MAX_VALUE, Long.MAX_VALUE));
		assertEquals(1, cmp.compare(Long.MAX_VALUE, 0l));
		assertEquals(0, cmp.compare(Long.MIN_VALUE, Long.MIN_VALUE));
		assertEquals(1, cmp.compare(Long.MIN_VALUE, 0l));
		assertEquals(1, cmp.compare(Long.MIN_VALUE, Long.MAX_VALUE));

	}

}
