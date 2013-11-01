package com.barchart.store.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;

public abstract class ColumnListMapper<K, T> extends RowMapper<K, List<T>> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	protected abstract void encodeColumn(T obj, RowMutator<K> mutator)
			throws Exception;

	protected abstract T decodeColumn(StoreColumn<K> column) throws Exception;

	@Override
	public void encode(final List<T> objects, final RowMutator<K> mutator) {

		try {

			for (final T obj : objects) {
				encodeColumn(obj, mutator);
			}

		} catch (final Exception e) {
			log.warn("Error encoding columns", e);
		}

	}

	@Override
	public List<T> decode(final StoreRow<K> row) {

		try {

			final List<T> records = new ArrayList<T>();

			for (final K col : row.columns()) {
				records.add(decodeColumn(row.get(col)));
			}

			return records;

		} catch (final Exception e) {
			log.warn("Error decoding columns", e);
		}

		return null;

	}
}
