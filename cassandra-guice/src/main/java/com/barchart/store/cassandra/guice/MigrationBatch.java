package com.barchart.store.cassandra.guice;

import rx.Observable;

import com.barchart.store.api.Batch;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.Table;

public class MigrationBatch implements Batch {

	private final Batch batch;
	private final Batch copy;

	public MigrationBatch(final Batch batch_, final Batch copy_) {
		batch = batch_;
		copy = copy_;
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> RowMutator<C> row(final Table<R, C, V> table, final R key) {
		return new MigrationRowMutator<C>(this, batch.row(table, key), copy.row(table, key));
	}

	@Override
	public Observable<Boolean> commit() {
		copy.commit();
		return batch.commit();
	}

}
