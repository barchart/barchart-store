package com.barchart.store.model.base;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
		if (value == null) {
			encoded = null;
		} else {
			try {
				encoded = ByteBuffer.wrap(value.getBytes("UTF-8"));
			} catch (final UnsupportedEncodingException e) {
				encoded = null;
			}
		}
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final List<String> list) {

		dirty();

		if (list == null) {
			encoded = null;
		} else {

			try {

				int length = 0;
				final byte[][] bytes = new byte[list.size()][];

				for (int i = 0; i < list.size(); i++) {

					bytes[i] = list.get(i).getBytes("UTF-8");

					if (bytes[i].length > 65535) {
						throw new IllegalArgumentException(
								"String value too large: " + bytes[i].length
										+ " (65535 max)");
					}

					length += 2 + bytes[i].length;

				}

				encoded = ByteBuffer.allocate(length);

				for (final byte[] b : bytes) {
					encoded.putShort((short) b.length);
					encoded.put(b);
				}

				encoded.flip();

			} catch (final UnsupportedEncodingException e) {
				encoded = null;
			}

		}

		return (T) this;

	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Boolean value) {
		dirty();
		if (value == null) {
			encoded = null;
		} else {
			encoded = ByteBuffer.allocate(1);
			encoded.put(value ? (byte) 1 : (byte) 0);
			encoded.flip();
		}
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Long value) {
		dirty();
		if (value == null) {
			encoded = null;
		} else {
			encoded = ByteBuffer.allocate(8);
			encoded.putLong(value);
			encoded.flip();
		}
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Integer value) {
		dirty();
		if (value == null) {
			encoded = null;
		} else {
			encoded = ByteBuffer.allocate(4);
			encoded.putInt(value);
			encoded.flip();
		}
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Double value) {
		dirty();
		if (value == null) {
			encoded = null;
		} else {
			encoded = ByteBuffer.allocate(8);
			encoded.putDouble(value);
			encoded.flip();
		}
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final Date value) {
		dirty();
		if (value == null) {
			encoded = null;
		} else {
			set(value.getTime());
		}
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final ByteBuffer value) {
		dirty();
		if (value == null) {
			encoded = null;
		} else {
			encoded = ByteBuffer.allocate(value.capacity());
			value.rewind();
			encoded.put(value);
			value.rewind();
			encoded.flip();
		}
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

	@SuppressWarnings("unchecked")
	@Override
	public List<String> asStringList() {

		if (last != null && last instanceof List) {
			return (List<String>) last;
		}

		if (encoded == null) {
			return null;
		}

		final List<String> list = new ArrayList<String>();

		while (encoded.hasRemaining()) {

			final short len = encoded.getShort();

			if (len < 0) {
				throw new IllegalStateException("Got invalid length marker: "
						+ len);
			}

			list.add(Charset.forName("UTF-8")
					.decode((ByteBuffer) encoded.slice().limit(len)).toString());

			encoded.position(encoded.position() + len);

		}

		encoded.rewind();
		last = list;

		return list;

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