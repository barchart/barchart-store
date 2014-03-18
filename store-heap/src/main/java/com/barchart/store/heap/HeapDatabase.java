package com.barchart.store.heap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.Table;

public class HeapDatabase {

	private final String databaseName;

	private final Map<String, HeapTable<?, ?, ?>> tableMap;

	public HeapDatabase(final String databaseName) {
		this.databaseName = databaseName;
		this.tableMap = new HashMap<String, HeapTable<?, ?, ?>>();
	}

	public <R extends Comparable<R>, C extends Comparable<C>, V> boolean has(final Table<R, C, V> table)
			throws Exception {
		return tableMap.containsKey(table.name());
	}

	public <R extends Comparable<R>, C extends Comparable<C>, V> void create(final Table<R, C, V> table)
			throws Exception {
		final HeapTable<R, C, V> heapTable = table(table);
		tableMap.put(table.name(), heapTable);
	}

	public <R extends Comparable<R>, C extends Comparable<C>, V> void update(final Table<R, C, V> table)
			throws Exception {
		throw new UnsupportedOperationException(
				"Drop and recreate table to change type or column definitions");
	}

	private <R extends Comparable<R>, C extends Comparable<C>, V> HeapTable<R, C, V> table(final Table<R, C, V> table) {
		if (table.columns().size() > 0) {
			for (final Table.Column<?> c : table.columns()) {
				if (c.isIndexed()) {
					return new IndexedHeapTable<R, C, V>(table);
				}
			}
		}
		return new HeapTable<R, C, V>(table);
	}

	/*
	 * public <R extends Comparable<R>> void create(final Table<R, String,
	 * String> table, final ColumnDef... columns) throws Exception { final
	 * HeapTable<R, String, String> heapTable = new IndexedHeapTable<R,
	 * String>(columns); tableMap.put(table.name(), heapTable); }
	 *
	 * public <R extends Comparable<R>> void update(final Table<R, String,
	 * String> table, final ColumnDef... columns) throws Exception { throw new
	 * UnsupportedOperationException(
	 * "Drop and recreate table to change type or column definitions"); }
	 */

	public <R extends Comparable<R>, C extends Comparable<C>, V> void truncate(final Table<R, C, V> table)
			throws Exception {
		tableMap.get(table.name()).truncate();
	}

	public <R extends Comparable<R>, C extends Comparable<C>, V> void delete(final Table<R, C, V> table)
			throws Exception {
		tableMap.remove(table.name());
	}

	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableQueryBuilder<R, C> fetch(
			final Table<R, C, V> table,
			final R... keys) throws Exception {
		return get(table).fetch(keys);
	}

	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableIndexQueryBuilder<R, C> query(
			final Table<R, C, V> table)
			throws Exception {
		return get(table).query();
	}

	public Batch batch() {
		return new BatchImpl();
	}

	private <R extends Comparable<R>, C extends Comparable<C>, V> HeapTable<R, C, V> get(final Table<R, C, V> table) {
		try {
			@SuppressWarnings("unchecked")
			final HeapTable<R, C, V> ht =
					(HeapTable<R, C, V>) tableMap.get(table.name());
			if (ht == null) {
				throw new IllegalStateException("No table \"" + table.name()
						+ "\" found in database: \"" + databaseName + "\"");
			}
			return ht;
		} catch (final ClassCastException cce) {
			throw new IllegalStateException("Wrong table type for \""
					+ table.name() + "\" in database \"" + databaseName + "\"");
		}
	}

	private final class BatchImpl implements Batch {

		private final List<HeapRowMutator<?, ?>> mutators;

		public BatchImpl() {
			mutators = new ArrayList<HeapRowMutator<?, ?>>();
		}

		@Override
		public <R extends Comparable<R>, C extends Comparable<C>, V> RowMutator<C> row(final Table<R, C, V> table,
				final R key) throws Exception {
			final HeapRowMutator<R, C> mutator = get(table).mutator(key);
			mutators.add(mutator);
			return mutator;
		}

		@Override
		public void commit() throws Exception {
			for (final HeapRowMutator<?, ?> mutator : mutators) {
				mutator.apply();
			}
		}

	}

}
