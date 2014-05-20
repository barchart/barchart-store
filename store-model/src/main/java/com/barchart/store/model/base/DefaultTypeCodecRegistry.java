package com.barchart.store.model.base;

import static com.barchart.store.model.base.TypeCodec.*;

public class DefaultTypeCodecRegistry implements TypeCodecRegistry {

	public final static DefaultTypeCodecRegistry INSTANCE = new DefaultTypeCodecRegistry();

	protected final static TypeCodec<?>[] codecs = new TypeCodec<?>[] {
			STRING, INTEGER, LONG, DOUBLE, BOOLEAN, BYTE_BUFFER, DATE
	};

	@Override
	@SuppressWarnings("unchecked")
	public <U> TypeCodec<? super U> forClass(final Class<U> type) {

		for (final TypeCodec<?> codec : codecs) {
			if (codec.type().isAssignableFrom(type)) {
				return (TypeCodec<? super U>) codec;
			}
		}

		return null;

	}

	@Override
	public TypeCodec<?> forCode(final short code) {

		for (final TypeCodec<?> codec : codecs) {
			if (codec.code() == code) {
				return codec;
			}
		}

		return null;

	}

}