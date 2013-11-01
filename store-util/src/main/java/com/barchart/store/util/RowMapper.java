package com.barchart.store.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public T call(final StoreRow<K> row) {
		try {
			return decode(row);
		} catch (final Exception e) {
			log.debug("Could not decode row", e);
			return null;
		}
	}

	public abstract void encode(final T obj, final RowMutator<K> mutator)
			throws Exception;

	public abstract T decode(final StoreRow<K> row) throws Exception;

	protected ObjectMapper mapper() {
		return RowMapper.mapper;
	}

}
