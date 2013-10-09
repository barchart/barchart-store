package com.barchart.store.heap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;

import com.barchart.store.api.StoreColumn;
import com.google.common.base.Charsets;

class HeapColumn<K> implements StoreColumn<K> {

	private static final Charset UTF8 = Charsets.UTF_8;

	private final K name;

	private ByteBuffer data = null;
	private long timestamp = 0;

	public HeapColumn(final K name_) {
		name = name_;
	}

	public HeapColumn(final K name, final String value) {
		this(name);
		set(value);
	}

	public HeapColumn(final K name, final double value) {
		this(name);
		set(value);
	}

	public HeapColumn(final K name, final int value) {
		this(name);
		set(value);
	}

	public HeapColumn(final K name, final long value) {
		this(name);
		set(value);
	}

	public HeapColumn(final K name, final boolean value) {
		this(name);
		set(value);
	}

	public HeapColumn(final K name, final Date value) {
		this(name);
		set(value);
	}

	public HeapColumn(final K name, final ByteBuffer value) {
		this(name);
		set(value);
	}

	protected void set(final String value) {
		data = ByteBuffer.wrap(value.getBytes(UTF8));
		timestamp = System.currentTimeMillis();
	}

	protected void set(final double value) {
		data = ByteBuffer.allocate(4);
		data.putDouble(value);
		timestamp = System.currentTimeMillis();
	}

	protected void set(final int value) {
		data = ByteBuffer.allocate(4);
		data.putInt(value);
		timestamp = System.currentTimeMillis();
	}

	protected void set(final long value) {
		data = ByteBuffer.allocate(8);
		data.putLong(value);
		timestamp = System.currentTimeMillis();
	}

	protected void set(final boolean value) {
		data = ByteBuffer.allocate(1);
		data.put(value ? (byte) 1 : (byte) 0);
		timestamp = System.currentTimeMillis();
	}

	protected void set(final Date value) {
		data = ByteBuffer.allocate(8);
		data.putLong(value.getTime());
		timestamp = System.currentTimeMillis();
	}

	protected void set(final ByteBuffer value) {
		data = ByteBuffer.allocate(value.capacity());
		value.rewind();
		data.put(value);
		value.rewind();
		data.flip();
		timestamp = System.currentTimeMillis();
	}

	@Override
	public K getName() {
		return name;
	}

	@Override
	public String getString() throws Exception {
		if (data == null) {
			return null;
		}
		return UTF8.decode(data).toString();
	}

	@Override
	public Double getDouble() throws Exception {
		if (data == null) {
			return null;
		}
		return data.getDouble();
	}

	@Override
	public Integer getInt() throws Exception {
		if (data == null) {
			return null;
		}
		return data.getInt();
	}

	@Override
	public Long getLong() throws Exception {
		if (data == null) {
			return null;
		}
		return data.getLong();
	}

	@Override
	public long getTimestamp() throws Exception {
		return timestamp;
	}

	@Override
	public Boolean getBoolean() throws Exception {
		if (data == null) {
			return null;
		}
		return data.get() == (byte) 1;
	}

	@Override
	public Date getDate() throws Exception {
		if (data == null) {
			return null;
		}
		return new Date(data.getLong());
	}

	@Override
	public ByteBuffer getBlob() throws Exception {
		final ByteBuffer clone = ByteBuffer.allocate(data.capacity());
		data.rewind();
		clone.put(data);
		data.rewind();
		clone.flip();
		return clone;
	}

}