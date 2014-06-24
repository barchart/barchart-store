package com.barchart.store.heap;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreLock;
import com.barchart.store.api.StoreLockService;
import com.barchart.store.api.Table;
import com.barchart.store.util.UUIDUtil;

/**
 * A lock service based on a heap store. Because HeapStore is not actually distributed, localLock() and globalLock()
 * have identical behavior.
 */
public class HeapLockProvider implements StoreLockService {

	protected final HeapStore store;
	protected final String keyspace;
	protected final Table<String, UUID, Long> table;

	public HeapLockProvider(final HeapStore store_, final String keyspace_, final Table<String, UUID, Long> table_) {
		store = store_;
		keyspace = keyspace_;
		table = table_;
	}

	@Override
	public Lock localLock(final String name, final long expiration, final TimeUnit units) {
		return new HeapLock(name, expiration, units);
	}

	@Override
	public Lock localLock(final String name) {
		return localLock(name, 60, TimeUnit.SECONDS);
	}

	@Override
	public Lock globalLock(final String name, final long expiration, final TimeUnit units) {
		return new HeapLock(name, expiration, units);
	}

	@Override
	public Lock globalLock(final String name) {
		return globalLock(name, 60, TimeUnit.SECONDS);
	}

	/**
	 * Enforces a slight delay between lock polls to avoid consuming busy-spin consuming all CPU. Remote store locks do
	 * not have this issue since they are network-constrained.
	 */
	protected class HeapLock extends StoreLock {

		protected HeapLock(final String name_, final long expiration_, final TimeUnit units_) {
			this(name_, expiration_, units_, 10);
		}

		protected HeapLock(final String name_, final long expiration_, final TimeUnit units_,
				final int interval_) {
			super(UUIDUtil.timeUUID(), name_, expiration_, units_, interval_);
		}

		@Override
		protected RowMutator<UUID> row(final String name) throws Exception {
			return store.batch(keyspace).row(table, name);
		}

		@Override
		protected ObservableQueryBuilder<String, UUID> fetch(final String name) throws Exception {
			return store.fetch(keyspace, table, name);
		}

	}

}
