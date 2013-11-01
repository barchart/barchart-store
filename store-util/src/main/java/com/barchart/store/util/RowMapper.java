package com.barchart.store.util;

import rx.util.functions.Func1;

import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreRow;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class RowMapper<K, T> implements Func1<StoreRow<K>, T> {

	private static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setVisibilityChecker(mapper.getSerializationConfig()
				.getDefaultVisibilityChecker()
				.withFieldVisibility(JsonAutoDetect.Visibility.ANY)
				.withGetterVisibility(JsonAutoDetect.Visibility.NONE)
				.withSetterVisibility(JsonAutoDetect.Visibility.NONE)
				.withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
	}

	@Override
	public T call(final StoreRow<K> row) {
		return decode(row);
	}

	public abstract void encode(final T obj, final RowMutator<K> mutator);

	public abstract T decode(final StoreRow<K> row);

	protected ObjectMapper mapper() {
		return RowMapper.mapper;
	}

}
