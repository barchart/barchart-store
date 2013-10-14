package com.barchart.store.heap;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.ObservableQueryBuilder;
import com.google.common.collect.MapMaker;

/**
 * Experimental, not for production.
 */
public class IndexedHeapTable<V> extends HeapTable<String, V> {

	private final Map<String, Map<Object, Collection<HeapRow<String>>>> indexes;

	public IndexedHeapTable(final ColumnDef... columns_) {

		super(columns_);

		indexes =
				new ConcurrentHashMap<String, Map<Object, Collection<HeapRow<String>>>>();

		if (columns_ != null && columns_.length > 0) {
			for (final ColumnDef def : columns_) {
				columns.put(def.key(), def);
				if (def.isIndexed()) {
					indexes.put(
							def.key(),
							new ConcurrentHashMap<Object, Collection<HeapRow<String>>>());
				}
			}
		}

	}

	@Override
	public ObservableQueryBuilder<String> query(final String column,
			final Object value) throws Exception {

		final Collection<HeapRow<String>> matches =
				indexes.get(column).get(value);

		if (matches != null) {
			return new HeapQueryBuilder<String>(matches);
		}

		return super.query(column, value);

	}

	@Override
	protected HeapRow<String> remove(final String key) {
		return deindex(super.remove(key));
	}

	@Override
	protected HeapRow<String> put(final String key, final HeapRow<String> row) {
		final HeapRow<String> old = super.put(key, row);
		if (old != row && old != null) {
			deindex(old);
		}
		index(row);
		return old;
	}

	private HeapRow<String> index(final HeapRow<String> row) {
		if (row != null) {
			for (final Map.Entry<String, Map<Object, Collection<HeapRow<String>>>> idx : indexes
					.entrySet()) {
				if (row.columns().contains(idx.getKey())) {
					update(row, row.getImpl(idx.getKey()));
				}
			}
		}
		return row;
	}

	private HeapRow<String> deindex(final HeapRow<String> row) {
		if (row != null) {
			for (final String name : row.columns()) {
				if (row.columns().contains(name)) {
					remove(row, row.getImpl(name));
				}
			}
		}
		return row;
	}

	protected void update(final HeapRow<String> row,
			final HeapColumn<String> column) {

		if (column == null) {
			return;
		}

		final ColumnDef def = columns.get(column.getName());
		final Map<Object, Collection<HeapRow<String>>> idx =
				indexes.get(column.getName());

		try {

			// Remove old value / de-index
			final HeapColumn<String> old = row.getImpl(column.getName());
			if (old != null && old != column) {
				remove(row, old);
			}
			row.update(column);

			final Class<?> type = def.type();

			if (type == String.class) {
				addIndex(idx, column.getString(), row);
			} else if (type == byte[].class) {
				// TODO compare byte arrays
			} else if (type == Boolean.class) {
				addIndex(idx, column.getBoolean(), row);
			} else if (type == ByteBuffer.class) {
				// TODO compare byte arrays
			} else if (type == Double.class) {
				addIndex(idx, column.getDouble(), row);
			} else if (type == Integer.class) {
				addIndex(idx, column.getInt(), row);
			} else if (type == Long.class) {
				addIndex(idx, column.getLong(), row);
			} else if (type == Date.class) {
				addIndex(idx, column.getDate(), row);
			}

		} catch (final Exception e) {
		}

	}

	protected void remove(final HeapRow<String> row,
			final HeapColumn<String> column) {

		if (column == null) {
			return;
		}

		final ColumnDef def = columns.get(column.getName());
		final Map<Object, Collection<HeapRow<String>>> idx =
				indexes.get(column.getName());

		try {

			final Class<?> type = def.type();

			if (type == String.class) {
				removeIndex(idx, column.getString(), row);
			} else if (type == byte[].class) {
				// TODO compare byte arrays
			} else if (type == Boolean.class) {
				removeIndex(idx, column.getBoolean(), row);
			} else if (type == ByteBuffer.class) {
				// TODO compare byte arrays
			} else if (type == Double.class) {
				removeIndex(idx, column.getDouble(), row);
			} else if (type == Integer.class) {
				removeIndex(idx, column.getInt(), row);
			} else if (type == Long.class) {
				removeIndex(idx, column.getLong(), row);
			} else if (type == Date.class) {
				removeIndex(idx, column.getDate(), row);
			}

		} catch (final Exception e) {
		}

	}

	private <T> void addIndex(
			final Map<Object, Collection<HeapRow<String>>> map, final T value,
			final HeapRow<String> row) {

		Collection<HeapRow<String>> matches = map.get(value);

		if (matches == null) {

			// Weak references to throw out deleted rows
			final Map<HeapRow<String>, Boolean> backingMap =
					new MapMaker().weakValues().makeMap();

			matches = Collections.newSetFromMap(backingMap);

			map.put(value, matches);

		}

		matches.add(row);

	}

	private <T> void removeIndex(
			final Map<Object, Collection<HeapRow<String>>> map, final T value,
			final HeapRow<String> row) {

		final Collection<HeapRow<String>> matches = map.get(value);

		if (matches != null) {
			matches.remove(row);
		}

	}

}