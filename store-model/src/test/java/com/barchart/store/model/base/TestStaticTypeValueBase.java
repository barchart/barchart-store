package com.barchart.store.model.base;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class TestStaticTypeValueBase {

	TestValue value;

	@Before
	public void setUp() throws Exception {
		value = new TestValue();
	}

	@Test
	public void testInt() throws Exception {
		value.set(10);
		assertEquals(Integer.class, value.get().getClass());
		assertEquals(10, (int) (Integer) value.get());
	}

	@Test
	public void testLong() throws Exception {
		value.set(10000000l);
		assertEquals(Long.class, value.get().getClass());
		assertEquals(10000000l, (long) (Long) value.get());
	}

	@Test
	public void testDouble() throws Exception {
		value.set(9.9);
		assertEquals(Double.class, value.get().getClass());
		assertEquals(9.9, (Double) value.get(), .00001);
	}

	@Test
	public void testDate() throws Exception {
		value.set(new DateTime(10));
		assertEquals(DateTime.class, value.get().getClass());
		assertEquals(10, ((DateTime) value.get()).getMillis());
	}

	@Test
	public void testBool() throws Exception {
		value.set(true);
		assertEquals(Boolean.class, value.get().getClass());
		assertEquals(true, value.get());
	}

	@Test
	public void testString() throws Exception {
		value.set("XXX");
		assertEquals(String.class, value.get().getClass());
		assertEquals("XXX", value.get());
	}

	@Test
	public void testBlob() throws Exception {
		value.set(ByteBuffer.wrap(new byte[] {
				1, 2, 3
		}));
		assertTrue(ByteBuffer.class.isAssignableFrom(value.get().getClass()));
		assertArrayEquals(new byte[] {
				1, 2, 3
		}, ((ByteBuffer) value.get()).array());
	}

	public class TestValue extends StaticTypedValueBase<TestValue> {
	}

}
