package com.barchart.store.cassandra;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreLock;
import com.barchart.store.api.StoreLockService;
import com.barchart.store.api.Table;
import com.barchart.store.util.UUIDUtil;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Implementation of a distributed lock on top of Cassandra. This is a
 * convenient locking mechanism for applications that are already using
 * Cassandra, but it does not scale efficiently compare to the alternatives.
 * Compared to solutions like Zookeeper or Hazelcast which proactively notify
 * clients, acquiring a lock requires constant polling of Cassandra if the
 * initial lock acquisition fails, so it should not be used for high-contention
 * locking scenarios to avoid DDOS-ing the cluster with lock clients. It is best
 * used as a coordination safety net for low-traffic locking tasks.
 *
 * The provided locking table should use wide rows with the following data
 * validators in Cassandra:
 *
 * row key = String (lock name)
 *
 * column name = UUID (unique lock client)
 *
 * column value = Long (lock expiration)
 */
public class CassandraLockProvider implements StoreLockService {

	protected final CassandraStore store;
	protected final String keyspace;
	protected final Table<String, UUID, Long> table;

	public CassandraLockProvider(final CassandraStore store_, final String keyspace_,
			final Table<String, UUID, Long> table_) {
		store = store_;
		keyspace = keyspace_;
		table = table_;
	}

	@Override
	public Lock localLock(final String name, final long expiration, final TimeUnit units) {
		return new CassandraLock(name, ConsistencyLevel.CL_LOCAL_QUORUM, expiration, units);
	}

	@Override
	public Lock localLock(final String name) {
		return localLock(name, 60, TimeUnit.SECONDS);
	}

	@Override
	public Lock globalLock(final String name, final long expiration, final TimeUnit units) {
		return new CassandraLock(name, ConsistencyLevel.CL_EACH_QUORUM, expiration, units);
	}

	@Override
	public Lock globalLock(final String name) {
		return globalLock(name, 60, TimeUnit.SECONDS);
	}

	protected class CassandraLock extends StoreLock {

		private final ConsistencyLevel level;

		protected CassandraLock(final String name_, final ConsistencyLevel level_, final long expiration_,
				final TimeUnit units_) {
			this(name_, level_, expiration_, units_, -1);
		}

		protected CassandraLock(final String name_, final ConsistencyLevel level_, final long expiration_,
				final TimeUnit units_, final int checkInterval_) {
			super(UUIDUtil.timeUUID(), name_, expiration_, units_, checkInterval_);
			level = level_;
		}

		@Override
		protected RowMutator<UUID> row(final String name) throws Exception {
			return store.batch(keyspace, level).row(table, name);
		}

		@Override
		protected ObservableQueryBuilder<String, UUID> fetch(final String name) throws Exception {
			return store.fetch(keyspace, table, ConsistencyLevel.CL_QUORUM, name);
		}

	}

}
