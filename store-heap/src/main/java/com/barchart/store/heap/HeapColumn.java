package com.barchart.store.heap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;

import com.barchart.store.api.StoreColumn;
import com.google.common.base.Charsets;

public class HeapColumn<K extends Comparable<K>> implements StoreColumn<K> {

	private static final Charset UTF8 = Charsets.UTF_8;

	private final K name;

	private ByteBuffer data = null;
	private long timestamp = 0;
	private int ttl = 0;

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
		if (value == null) {
			data = null;
		} else {
			data = ByteBuffer.wrap(value.getBytes(UTF8));
		}
		timestamp = System.currentTimeMillis();
	}

	protected void set(final double value) {
		data = ByteBuffer.allocate(8);
		data.putDouble(value);
		data.flip();
		timestamp = System.currentTimeMillis();
	}

	protected void set(final int value) {
		data = ByteBuffer.allocate(4);
		data.putInt(value);
		data.flip();
		timestamp = System.currentTimeMillis();
	}

	protected void set(final long value) {
		data = ByteBuffer.allocate(8);
		data.putLong(value);
		data.flip();
		timestamp = System.currentTimeMillis();
	}

	protected void set(final boolean value) {
		data = ByteBuffer.allocate(1);
		data.put(value ? (byte) 1 : (byte) 0);
		data.flip();
		timestamp = System.currentTimeMillis();
	}

	protected void set(final Date value) {
		if (value == null) {
			data = null;
		} else {
			data = ByteBuffer.allocate(8);
			data.putLong(value.getTime());
			data.flip();
		}
		timestamp = System.currentTimeMillis();
	}

	protected void set(final ByteBuffer value) {
		if (value == null) {
			data = null;
		} else {
			data = ByteBuffer.allocate(value.capacity());
			value.rewind();
			data.put(value);
			value.rewind();
			data.flip();
		}
		timestamp = System.currentTimeMillis();
	}

	@Override
	public K getName() {
		return name;
	}

	@Override
	public String getString() {
		if (data == null) {
			return null;
		}
		final String value = UTF8.decode(data).toString();
		data.rewind();
		return value;
	}

	@Override
	public Double getDouble() {
		if (data == null) {
			return null;
		}
		final double value = data.getDouble();
		data.rewind();
		return value;
	}

	@Override
	public Integer getInt() {
		if (data == null) {
			return null;
		}
		final int value = data.getInt();
		data.rewind();
		return value;
	}

	@Override
	public Long getLong() {
		if (data == null) {
			return null;
		}
		final long value = data.getLong();
		data.rewind();
		return value;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public Boolean getBoolean() {
		if (data == null) {
			return null;
		}
		final boolean value = (data.get() == (byte) 1);
		data.rewind();
		return value;
	}

	@Override
	public Date getDate() {
		if (data == null) {
			return null;
		}
		return new Date(getLong());
	}

	@Override
	public ByteBuffer getBlob() {
		final ByteBuffer clone = ByteBuffer.allocate(data.capacity());
		data.rewind();
		clone.put(data);
		data.rewind();
		clone.flip();
		return clone;
	}

	public HeapColumn<K> ttl(final int ttl_) {
		ttl = ttl_;
		return this;
	}

	public int ttl() {
		return ttl;
	}

	@Override
	public int compareTo(final StoreColumn<K> that) {
		return name.compareTo(that.getName());
	}

}