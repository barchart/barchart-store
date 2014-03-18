package com.barchart.store.heap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableQueryBuilder;

public class HeapTable<R extends Comparable<R>, C extends Comparable<C>, V> {

	protected final Map<R, HeapRow<R, C>> rows;
	protected final Map<C, ColumnDef> columns;

	public HeapTable(final ColumnDef... columns_) {
		rows = new ConcurrentHashMap<R, HeapRow<R, C>>();
		columns = new HashMap<C, ColumnDef>();
	}

	protected HeapRowMutator<R, C> mutator(final R key) {
		return new HeapRowMutator<R, C>(this, key);
	}

	public ObservableQueryBuilder<R, C> fetch(final R... keys)
			throws Exception {

		if (keys != null && keys.length > 0) {

			final List<HeapRow<R, C>> matches = new ArrayList<HeapRow<R, C>>();

			for (final R key : keys) {
				final HeapRow<R, C> row = rows.get(key);
				if (row != null) {
					matches.add(row);
				} else {
					// Per spec (and Cassandra behavior), we should always
					// return a row for an explicitly requested key, even if it
					// is empty
					matches.add(new HeapRow<R, C>(key));
				}
			}

			return new HeapQueryBuilder<R, C>(
					Collections.unmodifiableCollection(matches));

		} else {

			return new HeapQueryBuilder<R, C>(
					Collections.unmodifiableCollection(rows.values()));

		}

	}

	public ObservableIndexQueryBuilder<R, C> query() throws Exception {
		// Unsupported operation, no indexes
		return new HeapIndexQueryBuilder<R, C>(null);
	}

	public void truncate() {
		rows.clear();
	}

	protected HeapRow<R, C> remove(final R key) {
		return rows.remove(key);
	}

	protected HeapRow<R, C> get(final R key) {
		return rows.get(key);
	}

	protected HeapRow<R, C> put(final R key, final HeapRow<R, C> row) {
		return rows.put(key, row);
	}

}
