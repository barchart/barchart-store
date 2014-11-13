package com.barchart.store.model.base;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

public abstract class TypeCodec<T> {

	public final static TypeCodec<String> STRING = new StringCodec(1);
	public final static TypeCodec<Integer> INTEGER = new IntegerCodec(2);
	public final static TypeCodec<Long> LONG = new LongCodec(3);
	public final static TypeCodec<Double> DOUBLE = new DoubleCodec(4);
	public final static TypeCodec<Boolean> BOOLEAN = new BooleanCodec(5);
	public final static TypeCodec<ByteBuffer> BYTE_BUFFER = new ByteBufferCodec(6);
	public final static TypeCodec<DateTime> DATE = new DateCodec(7);

	private final short code;
	private final Class<? extends T> type;

	public TypeCodec(final int code_, final Class<? extends T> type_) {
		code = (short) code_;
		type = type_;
	}

	public abstract ByteBuffer allocate(T value);

	public abstract T read(ByteBuffer buffer);

	public abstract void write(ByteBuffer buffer, T value);

	public abstract T parse(String text);

	public String format(final Object value) {

		if (value == null)
			return null;

		if (!type.isAssignableFrom(value.getClass()))
			throw new IllegalArgumentException("Not a " + type);

		return value.toString();

	}

	public short code() {
		return code;
	}

	public Class<? extends T> type() {
		return type;
	}

	protected final ByteBuffer allocate(final T value, final int capacity) {

		if (value == null)
			return null;

		final ByteBuffer buffer = ByteBuffer.allocate(capacity + 2);
		buffer.putShort(code);
		write(buffer, value);
		buffer.flip();

		return buffer;

	}

	public static class StringCodec extends TypeCodec<String> {

		public StringCodec(final int code) {
			super(code, String.class);
		}

		@Override
		public String read(final ByteBuffer buffer) {
			return Charset.forName("UTF-8").decode(buffer).toString();
		}

		@Override
		public ByteBuffer allocate(final String value) {
			try {
				return allocate(value, value.getBytes("UTF-8").length);
			} catch (final UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void write(final ByteBuffer buffer, final String value) {
			try {
				buffer.put(value.getBytes("UTF-8"));
			} catch (final UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String parse(final String text) {
			if (text == null || text.isEmpty())
				return null;
			return text;
		}

	}

	public static class IntegerCodec extends TypeCodec<Integer> {

		public IntegerCodec(final int code) {
			super(code, Integer.class);
		}

		@Override
		public Integer read(final ByteBuffer buffer) {
			return buffer.getInt();
		}

		@Override
		public ByteBuffer allocate(final Integer value) {
			return allocate(value, 4);
		}

		@Override
		public void write(final ByteBuffer buffer, final Integer value) {
			buffer.putInt(value);
		}

		@Override
		public Integer parse(final String text) {
			if (text == null || text.isEmpty())
				return null;
			return Integer.parseInt(text);
		}

	}

	public static class LongCodec extends TypeCodec<Long> {

		public LongCodec(final int code) {
			super(code, Long.class);
		}

		@Override
		public Long read(final ByteBuffer buffer) {
			return buffer.getLong();
		}

		@Override
		public ByteBuffer allocate(final Long value) {
			return allocate(value, 8);
		}

		@Override
		public void write(final ByteBuffer buffer, final Long value) {
			buffer.putLong(value);
		}

		@Override
		public Long parse(final String text) {
			if (text == null || text.isEmpty())
				return null;
			return Long.parseLong(text);
		}

	}

	public static class DoubleCodec extends TypeCodec<Double> {

		public DoubleCodec(final int code) {
			super(code, Double.class);
		}

		@Override
		public Double read(final ByteBuffer buffer) {
			return buffer.getDouble();
		}

		@Override
		public ByteBuffer allocate(final Double value) {
			return allocate(value, 8);
		}

		@Override
		public void write(final ByteBuffer buffer, final Double value) {
			buffer.putDouble(value);
		}

		@Override
		public Double parse(final String text) {
			if (text == null || text.isEmpty())
				return null;
			return Double.parseDouble(text);
		}

		@Override
		public String format(final Object value) {

			if (value == null)
				return null;

			if (!(value instanceof Double))
				throw new IllegalArgumentException("Not a Double");

			return String.format("%f", value);

		}

	}

	public static class BooleanCodec extends TypeCodec<Boolean> {

		public BooleanCodec(final int code) {
			super(code, Boolean.class);
		}

		@Override
		public Boolean read(final ByteBuffer buffer) {
			return buffer.get() == 1;
		}

		@Override
		public ByteBuffer allocate(final Boolean value) {
			return allocate(value, 1);
		}

		@Override
		public void write(final ByteBuffer buffer, final Boolean value) {
			buffer.put(value ? (byte) 1 : 0);
		}

		@Override
		public Boolean parse(final String text) {
			if (text == null || text.isEmpty())
				return null;
			return text.equals("1") ||
					text.equalsIgnoreCase("true") ||
					text.equalsIgnoreCase("t");
		}

	}

	public static class ByteBufferCodec extends TypeCodec<ByteBuffer> {

		public ByteBufferCodec(final int code) {
			super(code, ByteBuffer.class);
		}

		@Override
		public ByteBuffer read(final ByteBuffer buffer) {
			final ByteBuffer clone = ByteBuffer.allocate(buffer.remaining());
			buffer.mark();
			clone.put(buffer);
			buffer.reset();
			clone.flip();
			return clone;
		}

		@Override
		public ByteBuffer allocate(final ByteBuffer value) {
			return allocate(value.asReadOnlyBuffer(), value.remaining());
		}

		@Override
		public void write(final ByteBuffer buffer, final ByteBuffer value) {
			buffer.put(value);
		}

		@Override
		public ByteBuffer parse(final String text) {
			if (text == null || text.isEmpty())
				return null;
			try {
				return ByteBuffer.wrap(text.getBytes("UTF-8"));
			} catch (final UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

	}

	public static class DateCodec extends TypeCodec<DateTime> {

		public DateCodec(final int code) {
			super(code, DateTime.class);
		}

		@Override
		public DateTime read(final ByteBuffer buffer) {
			return new DateTime(buffer.getLong(), ISOChronology.getInstanceUTC());
		}

		@Override
		public ByteBuffer allocate(final DateTime value) {
			return allocate(value, 8);
		}

		@Override
		public void write(final ByteBuffer buffer, final DateTime value) {
			buffer.putLong(value.getMillis());
		}

		@Override
		public DateTime parse(final String text) {
			if (text == null || text.isEmpty())
				return null;
			return new DateTime(Long.parseLong(text), ISOChronology.getInstanceUTC());
		}

	}

}