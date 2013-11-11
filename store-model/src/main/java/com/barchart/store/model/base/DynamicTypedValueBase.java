package com.barchart.store.model.base;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;

import com.barchart.store.model.api.DynamicTypedValue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicTypedValueBase<T extends DynamicTypedValue<T>> implements
		DynamicTypedValue<T> {

	@JsonIgnore
	private ByteBuffer encoded;

	private Object last;

	@JsonProperty
	public void value(final byte[] value) {
		if (value != null) {
			encoded = ByteBuffer.wrap(value);
		}
	}

	@JsonProperty
	public byte[] value() {

		byte[] value = null;

		if (encoded != null) {
			value = encoded.array();
			encoded.rewind();
		}

		return value;

	}

	private void dirty() {
		last = null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final String value) {
		dirty();
		try {
			encoded = ByteBuffer.wrap(value.getBytes("UTF-8"));
		} catch (final UnsupportedEncodingException e) {
			encoded = null;
		}
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Boolean value) {
		dirty();
		encoded = ByteBuffer.allocate(1);
		encoded.put(value ? (byte) 1 : (byte) 0);
		encoded.flip();
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Long value) {
		dirty();
		encoded = ByteBuffer.allocate(8);
		encoded.putLong(value);
		encoded.flip();
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Integer value) {
		dirty();
		encoded = ByteBuffer.allocate(4);
		encoded.putInt(value);
		encoded.flip();
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Double value) {
		dirty();
		encoded = ByteBuffer.allocate(8);
		encoded.putDouble(value);
		encoded.flip();
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Date value) {
		dirty();
		set(value.getTime());
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final ByteBuffer value) {
		dirty();
		encoded = ByteBuffer.allocate(value.capacity());
		value.rewind();
		encoded.put(value);
		value.rewind();
		encoded.flip();
		return (T) this;
	}

	@Override
	public String asString() {
		if (last != null && last instanceof String) {
			return (String) last;
		}
		if (encoded == null) {
			return null;
		}
		final String value =
				Charset.forName("UTF-8").decode(encoded).toString();
		encoded.rewind();
		last = value;
		return value;
	}

	@Override
	public Boolean asBoolean() {
		if (last != null && last instanceof Boolean) {
			return (Boolean) last;
		}
		if (encoded == null) {
			return null;
		}
		final boolean value = encoded.get() == (byte) 1;
		encoded.rewind();
		last = value;
		return value;
	}

	@Override
	public Long asLong() {
		if (last != null && last instanceof Long) {
			return (Long) last;
		}
		if (encoded == null) {
			return null;
		}
		final long value = encoded.getLong();
		encoded.rewind();
		last = value;
		return value;
	}

	@Override
	public Integer asInt() {
		if (last != null && last instanceof Integer) {
			return (Integer) last;
		}
		if (encoded == null) {
			return null;
		}
		final int value = encoded.getInt();
		encoded.rewind();
		last = value;
		return value;
	}

	@Override
	public Double asDouble() {
		if (last != null && last instanceof Double) {
			return (Double) last;
		}
		if (encoded == null) {
			return null;
		}
		final double value = encoded.getDouble();
		encoded.rewind();
		last = value;
		return value;
	}

	@Override
	public Date asDate() {
		if (last != null && last instanceof Date) {
			return (Date) last;
		}
		if (encoded == null) {
			return null;
		}
		final Date value = new Date(encoded.getLong());
		encoded.rewind();
		last = value;
		return value;
	}

	@Override
	public ByteBuffer asBlob() {
		if (encoded == null) {
			return null;
		}
		final ByteBuffer clone = ByteBuffer.allocate(encoded.capacity());
		encoded.rewind();
		clone.put(encoded);
		encoded.rewind();
		clone.flip();
		// Don't want to cache values with state that could be shared
		last = null;
		return clone;
	}

}