package com.barchart.store.model.base;

import static com.barchart.store.model.base.TypeCodec.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;

import com.barchart.store.model.api.DynamicTypedValue;

public class DynamicTypedValueBase<T extends DynamicTypedValue<T>> extends TypedValueBase implements
		DynamicTypedValue<T> {

	void dirty() {
		last = null;
	}

	@Override
	public T set(final String value) {
		try {
			return set(value, STRING, value == null ? 0 : value.getBytes("UTF-8").length);
		} catch (final UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(final List<String> list) {

		last = null;

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

	@Override
	public T set(final Boolean value) {
		return set(value, BOOLEAN, 1);
	}

	@Override
	public T set(final Long value) {
		return set(value, LONG, 8);
	}

	@Override
	public T set(final Integer value) {
		return set(value, INTEGER, 4);
	}

	@Override
	public T set(final Double value) {
		return set(value, DOUBLE, 8);
	}

	@Override
	public T set(final Date value) {
		return set(new DateTime(value == null ? 0l : value.getTime()), DATE, 8);
	}

	@Override
	public T set(final ByteBuffer value) {
		return set(value, BYTE_BUFFER, value == null ? 0 : value.remaining());
	}

	@SuppressWarnings("unchecked")
	public <U> T set(final U value, final TypeCodec<U> codec, final int size) {

		if (value == null) {
			encoded = null;
		} else {
			encoded = ByteBuffer.allocate(size);
			codec.write(encoded, value);
			encoded.flip();
		}

		last = null;
		// last = value;

		return (T) this;

	}

	@SuppressWarnings("unchecked")
	public <U> U get(final TypeCodec<U> codec) {

		if (last != null && codec.type().isInstance(last)) {
			return (U) last;
		}

		if (encoded == null) {
			return null;
		}

		encoded.mark();
		final U value = codec.read(encoded);
		encoded.reset();

		last = value;
		return value;

	}

	@Override
	public String asString() {
		return get(STRING);
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
		return get(BOOLEAN);
	}

	@Override
	public Long asLong() {
		return get(LONG);
	}

	@Override
	public Integer asInt() {
		return get(INTEGER);
	}

	@Override
	public Double asDouble() {
		return get(DOUBLE);
	}

	@Override
	public Date asDate() {
		return new Date(get(DATE).getMillis());
	}

	@Override
	public ByteBuffer asBlob() {
		return get(BYTE_BUFFER);
	}

}