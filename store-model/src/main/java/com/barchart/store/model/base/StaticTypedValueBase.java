package com.barchart.store.model.base;

import java.nio.ByteBuffer;

import com.barchart.store.model.api.StaticTypedValue;

public class StaticTypedValueBase<T extends StaticTypedValue<T>> extends TypedValueBase implements StaticTypedValue<T> {

	private TypeCodec<?> codec;

	protected StaticTypedValueBase() {
		super();
	}

	protected StaticTypedValueBase(final TypeCodecRegistry registry) {
		super(registry);
	}

	protected StaticTypedValueBase(final ByteBuffer buffer) {
		this(buffer, DefaultTypeCodecRegistry.INSTANCE);
	}

	protected StaticTypedValueBase(final ByteBuffer buffer, final TypeCodecRegistry registry) {

		super(registry);

		if (buffer != null) {

			buffer.mark();

			final short code = buffer.getShort();

			codec = codec(code);

			if (codec == null) {
				buffer.reset();
				throw new IllegalArgumentException("Unknown code " + code);
			}

			set(codec.read(buffer));

			buffer.reset();

		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> T set(final U value) {

		if (value == null) {

			encoded = null;
			codec = null;

		} else {

			final TypeCodec<? super U> codec = codec((Class<U>) value.getClass());

			if (codec == null) {
				throw new IllegalArgumentException("No type mapping for " + value.getClass());
			}

			encoded = codec.allocate(value);
			this.codec = codec;
		}

		last = value;

		return (T) this;

	}

	@Override
	public Object get() {

		if (last == null && encoded != null) {

			encoded.mark();

			final short bcode = encoded.getShort();

			codec = codec(bcode);

			if (codec == null) {
				throw new IllegalArgumentException("No type mapping for code " + bcode);
			}

			last = codec.read(encoded);

			encoded.reset();

		}

		return last;

	}

	@Override
	public Class<?> type() {

		final Object value = get();

		if (value != null) {
			return value.getClass();
		}

		return null;

	}

	@Override
	public ByteBuffer blob() {
		return encoded;
	}

	@Override
	public String toString() {

		final Object value = get();

		if (value == null)
			return "";

		if (codec == null)
			return value.toString();

		return codec.format(value);

	}

	@Override
	public boolean equals(final Object that) {

		if (getClass().equals(that.getClass())) {
			final StaticTypedValueBase<?> value = (StaticTypedValueBase<?>) that;
			return encoded == null ? encoded == value.encoded : encoded.equals(value.encoded);
		}

		return false;

	}

}