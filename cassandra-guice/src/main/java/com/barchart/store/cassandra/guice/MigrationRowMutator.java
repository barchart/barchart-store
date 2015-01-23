package com.barchart.store.cassandra.guice;

import java.nio.ByteBuffer;

import rx.Observable;

import com.barchart.store.api.Batch;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.Table;

public class MigrationRowMutator<T> implements RowMutator<T> {

	private final MigrationBatch batch;
	private final RowMutator<T> row;
	private final RowMutator<T> copy;

	MigrationRowMutator(final MigrationBatch batch_, final RowMutator<T> row_, final RowMutator<T> copy_) {
		batch = batch_;
		row = row_;
		copy = copy_;
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> RowMutator<C> row(final Table<R, C, V> table,
			final R key) {
		return batch.row(table, key);
	}

	@Override
	public Observable<Boolean> commit() {
		return batch.commit();
	}

	@Override
	public RowMutator<T> set(final T column, final String value) {
		row.set(column, value);
		copy.set(column, value);
		return this;
	}

	@Override
	public RowMutator<T> set(final T column, final double value) {
		row.set(column, value);
		copy.set(column, value);
		return this;
	}

	@Override
	public RowMutator<T> set(final T column, final ByteBuffer value) {
		row.set(column, value);
		copy.set(column, value);
		return this;
	}

	@Override
	public RowMutator<T> set(final T column, final int value) {
		row.set(column, value);
		copy.set(column, value);
		return this;
	}

	@Override
	public RowMutator<T> set(final T column, final long value) {
		row.set(column, value);
		copy.set(column, value);
		return this;
	}

	@Override
	public RowMutator<T> remove(final T column) {
		row.remove(column);
		copy.remove(column);
		return this;
	}

	@Override
	public RowMutator<T> ttl(final Integer ttl) {
		row.ttl(ttl);
		copy.ttl(ttl);
		return this;
	}

	@Override
	public Batch delete() {
		row.delete();
		copy.delete();
		return batch;
	}

}
