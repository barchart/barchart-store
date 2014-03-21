package com.barchart.store.heap;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.barchart.store.api.Batch;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.Table;

public class HeapRowMutator<R extends Comparable<R>, K extends Comparable<K>> implements RowMutator<K> {

	protected final HeapTable<R, K, ?> table;
	protected final Batch batch;
	protected final R key;
	protected final Set<HeapColumn<K>> update;
	protected final Set<K> remove;

	private int ttl = 0;

	private boolean delete = false;

	public HeapRowMutator(final HeapTable<R, K, ?> table_, final Batch batch_, final R key_) {
		table = table_;
		batch = batch_;
		key = key_;
		update = new HashSet<HeapColumn<K>>();
		remove = new HashSet<K>();
	}

	@Override
	public RowMutator<K> set(final K column, final String value) {
		update.add(new HeapColumn<K>(column, value).ttl(ttl));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final double value) {
		update.add(new HeapColumn<K>(column, value).ttl(ttl));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final int value) {
		update.add(new HeapColumn<K>(column, value).ttl(ttl));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final long value) {
		update.add(new HeapColumn<K>(column, value).ttl(ttl));
		return this;
	}

	@Override
	public RowMutator<K> set(final K column, final ByteBuffer value) {
		update.add(new HeapColumn<K>(column, value).ttl(ttl));
		return null;
	}

	@Override
	public RowMutator<K> ttl(final Integer ttl_) {
		ttl = ttl_;
		return this;
	}

	@Override
	public RowMutator<K> remove(final K column) {
		remove.add(column);
		return this;
	}

	@Override
	public Batch delete() {
		delete = true;
		return batch;
	}

	@Override
	public <S extends Comparable<S>, C extends Comparable<C>, V> RowMutator<C> row(final Table<S, C, V> table,
			final S key) {
		return batch.row(table, key);
	}

	@Override
	public void commit() throws Exception {
		batch.commit();
	}

	@SuppressWarnings("unchecked")
	protected void apply() {

		if (delete) {

			table.remove(key);

		} else {

			HeapRow<R, K> row = table.get(key);

			if (row == null) {
				// Force UUID to use time-based comparator for columns
				// TODO make alternate column comparators part of the Table API
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
