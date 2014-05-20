package com.barchart.store.model.base;

import java.nio.ByteBuffer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TypedValueBase {

	protected final TypeCodecRegistry codecs;

	protected TypedValueBase() {
		codecs = DefaultTypeCodecRegistry.INSTANCE;
	}

	protected TypedValueBase(final TypeCodecRegistry registry) {
		codecs = registry;
	}

	@JsonIgnore
	protected ByteBuffer encoded;

	protected Object last;

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

	public <U> TypeCodec<? super U> codec(final Class<U> type) {
		return codecs.forClass(type);
	}

	public TypeCodec<?> codec(final short code) {
		return codecs.forCode(code);
	}

}