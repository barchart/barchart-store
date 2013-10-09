package com.barchart.store.heap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreService.Table;

public class HeapDatabase {

	private final String databaseName;

	private final Map<String, HeapTable<?, ?>> tableMap;

	public HeapDatabase(final String databaseName) {
		this.databaseName = databaseName;
		this.tableMap = new HashMap<String, HeapTable<?, ?>>();
	}

	public <K, V> boolean has(final Table<K, V> table) throws Exception {
		return tableMap.containsKey(table.name);
	}

	public <K, V> void create(final Table<K, V> table) throws Exception {
		final HeapTable<K, V> heapTable = new HeapTable<K, V>();
		tableMap.put(table.name, heapTable);
	}

	public void create(final Table<String, String> table,
			final ColumnDef... columns) throws Exception {
		final HeapTable<String, String> heapTable =
				new IndexedHeapTable<String>(columns);
		tableMap.put(table.name, heapTable);
	}

	public <K, V> void update(final Table<K, V> table) throws Exception {
		throw new UnsupportedOperationException(
				"Drop and recreate table to change type or column definitions");
	}

	public void update(final Table<String, String> table,
			final ColumnDef... columns) throws Exception {
		throw new UnsupportedOperationException(
				"Drop and recreate table to change type or column definitions");
	}

	public <K, V> void delete(final Table<K, V> table) throws Exception {
		tableMap.remove(table.name);
	}

	public <K, V> ObservableQueryBuilder<K> fetch(final Table<K, V> table,
			final String... keys) throws Exception {
		return get(table).fetch(keys);
	}

	public <K, V> ObservableQueryBuilder<K> query(final Table<K, V> table,
			final K column, final Object value) throws Exception {
		return get(table).query(column, value);
	}

	public Batch batch() {
		return new BatchImpl();
	}

	private <K, V> HeapTable<K, V> get(final Table<K, V> table) {
		try {
			@SuppressWarnings("unchecked")
			final HeapTable<K, V> ht =
					(HeapTable<K, V>) tableMap.get(table.name);
			if (ht == null) {
				throw new IllegalStateException("No table \"" + table.name
						+ "\" found in database: \"" + databaseName + "\"");
			}
			return ht;
		} catch (final ClassCastException cce) {
			throw new IllegalStateException("Wrong table type for \""
					+ table.name + "\" in database \"" + databaseName + "\"");
		}
	}

	private final class BatchImpl implements Batch {

		private final List<HeapRowMutator<?>> mutators;

		public BatchImpl() {
			mutators = new ArrayList<HeapRowMutator<?>>();
		}

		@Override
		public <K, V> RowMutator<K> row(final Table<K, V> table,
				final String key) throws Exception {
			final HeapRowMutator<K> mutator = get(table).mutator(key);
			mutators.add(mutator);
			return mutator;
		}

		@Override
		public void commit() throws Exception {
			for (final HeapRowMutator<?> mutator : mutators) {
				mutator.apply();
			}
		}

	}

}
