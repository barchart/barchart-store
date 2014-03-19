package com.barchart.store.heap;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.barchart.store.api.RowMutator;

public class HeapRowMutator<R extends Comparable<R>, K extends Comparable<K>> implements RowMutator<K> {

	protected final HeapTable<R, K, ?> table;
	protected final R key;
	protected final Set<HeapColumn<K>> update;
	protected final Set<K> remove;

	private boolean delete = false;

	public HeapRowMutator(final HeapTable<R, K, ?> table_, final R key_) {
		table = table_;
		key = key_;
		update = new HashSet<HeapColumn<K>>();
		remove = new HashSet<K>();
	}

	@Override
	public RowMutator<K> set(final K column, final String value)
			throws Exception {
		update.add(new HeapColumn<K>(column, value));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final double value)
			throws Exception {
		update.add(new HeapColumn<K>(column, value));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final int value) throws Exception {
		update.add(new HeapColumn<K>(column, value));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final long value) throws Exception {
		update.add(new HeapColumn<K>(column, value));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final ByteBuffer value)
			throws Exception {
		update.add(new HeapColumn<K>(column, value));
		return null;
	}

	@Override
	public RowMutator<K> ttl(final Integer ttl) throws Exception {
		// TODO Implement TTL for individual columns
		return this;
	}

	@Override
	public RowMutator<K> remove(final K column) throws Exception {
		remove.add(column);
		return this;
	}

	@Override
	public void delete() throws Exception {
		delete = true;
	}

	@SuppressWarnings("unchecked")
	protected void apply() {

		if (delete) {

			table.remove(key);

		} else {

			HeapRow<R, K> row = table.get(key);

			if (row == null) {
				// Force UUID to use time-based comparator for columns
				if (table.definition().columnType() == UUID.class) {
					row = new HeapRow<R, K>(key, (Comparator<K>) HeapStore.UUID_COMPARATOR);
				} else {
					row = new HeapRow<R, K>(key);
				}
			}

			// Apply inserts/updates
			for (final HeapColumn<K> hc : update) {
				row.update(hc);
			}

			// Apply removals
			for (final K rm : remove) {
				row.delete(rm);
			}

			table.put(key, row);

		}

	}

}
