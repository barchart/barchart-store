package com.barchart.store.util;

import java.util.ArrayList;
import java.util.List;

import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;

public abstract class ColumnListMapper<R extends Comparable<R>, C extends Comparable<C>, T> extends
		RowMapper<R, C, List<T>> {

	protected abstract void encodeColumn(T obj, RowMutator<C> mutator)
			throws Exception;

	protected abstract T decodeColumn(StoreColumn<C> column) throws Exception;

	@Override
	public void encode(final List<T> objects, final RowMutator<C> mutator)
			throws Exception {

		for (final T obj : objects) {
			encodeColumn(obj, mutator);
		}

	}

	@Override
	public List<T> decode(final StoreRow<R, C> row) throws Exception {

		final List<T> records = new ArrayList<T>();

		for (final C col : row.columns()) {
			records.add(decodeColumn(row.get(col)));
		}

		return records;

	}

}
