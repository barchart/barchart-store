package com.barchart.store.heap;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import com.barchart.store.api.RowMutator;

public class HeapRowMutator<K> implements RowMutator<K> {

	protected final HeapTable<K, ?> table;
	protected final String key;
	protected final Set<HeapColumn<K>> update;
	protected final Set<K> remove;

	private boolean delete = false;

	public HeapRowMutator(final HeapTable<K, ?> table_, final String key_) {
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

	protected void apply() {

		if (delete) {

			table.remove(key);

		} else {

			HeapRow<K> row = table.get(key);

			if (row == null) {
				row = new HeapRow<K>(key);
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
