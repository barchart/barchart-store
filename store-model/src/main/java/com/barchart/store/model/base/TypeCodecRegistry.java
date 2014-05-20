package com.barchart.store.model.base;


public interface TypeCodecRegistry {

	<U> TypeCodec<? super U> forClass(final Class<U> type);

	TypeCodec<?> forCode(final short code);

}