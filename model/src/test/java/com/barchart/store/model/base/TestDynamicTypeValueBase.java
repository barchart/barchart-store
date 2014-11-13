package com.barchart.store.model.base;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestDynamicTypeValueBase {

	TestValue value;

	@Before
	public void setUp() throws Exception {
		value = new TestValue();
	}

	@Test
	public void testInt() throws Exception {
		value.set(10);
		assertEquals(10, (int) value.asInt());
	}

	@Test
	public void testLong() throws Exception {
		value.set(10000000l);
		assertEquals(10000000l, (long) value.asLong());
	}

	@Test
	public void testDouble() throws Exception {
		value.set(9.9);
		assertEquals(9.9, value.asDouble(), .00001);
	}

	@Test
	public void testDate() throws Exception {
		value.set(new Date(10));
		assertEquals(10, value.asDate().getTime());
	}

	@Test
	public void testBool() throws Exception {
		value.set(true);
		assertEquals(true, value.asBoolean());
	}

	@Test
	public void testString() throws Exception {
		value.set("XXX");
		assertEquals("XXX", value.asString());
	}

	@Test
	public void testStringList() throws Exception {
		final List<String> list = new ArrayList<String>();
		list.add("a");
		list.add("b");
		list.add("c");
		list.add("d");
		list.add("e");
		list.add("f");
		value.set(list);
		final List<String> decoded = value.asStringList();
		assertEquals(list.size(), decoded.size());
		for (int i = 0; i < list.size(); i++) {
			assertEquals(list.get(i), decoded.get(i));
		}
	}

	@Test
	public void testBlob() throws Exception {
		value.set(ByteBuffer.wrap(new byte[] {
				1, 2, 3
		}));
		assertArrayEquals(new byte[] {
				1, 2, 3
		}, value.asBlob().array());
	}

	@Test
	public void testSerialize() throws Exception {

		final ObjectMapper mapper = new ObjectMapper();

		value.set("XXX");

		final String ser = mapper.writeValueAsString(value);

		final TestValue deser = mapper.readValue(ser, TestValue.class);

		assertEquals("XXX", deser.asString());

	}

	public static class TestValue extends DynamicTypedValueBase<TestValue> {
	}

}
