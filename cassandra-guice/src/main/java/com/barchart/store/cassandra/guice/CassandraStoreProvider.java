package com.barchart.store.cassandra.guice;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import rx.Observable;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreLockService;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.Table;
import com.barchart.store.cassandra.CassandraLockProvider;
import com.barchart.store.cassandra.CassandraStore;
import com.barchart.util.guice.Activate;
import com.barchart.util.guice.Component;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.typesafe.config.Config;

@Component("com.barchart.store.cassandra")
public class CassandraStoreProvider implements StoreService, StoreLockService {

	private final CassandraStore store;
	private CassandraLockProvider locks;

	@Inject
	@Named("#")
	private Config config;

	public CassandraStoreProvider() {
		store = new CassandraStore();
	}

	@Activate
	public void activate() {

		if (config.hasPath("seeds")) {
			store.setSeeds(config.getStringList("seeds").toArray(new String[] {}));
		}

		if (config.hasPath("cluster")) {
			store.setCluster(config.getString("cluster"));
		}

		if (config.hasPath("strategyClass")) {
			store.setStrategy(config.getString("strategyClass"));
		}

		if (config.hasPath("username")) {
			store.setCredentials(config.getString("username"), config.getString("pass"));
		}

		if (config.hasPath("zones")) {

			final List<String> zonesList = config.getStringList("zones");

			if (zonesList.size() > 0) {
				store.setZones(zonesList.toArray(new String[] {}));
			}

		}

		if (config.hasPath("replicationFactor")) {
			store.setReplicationFactor(config.getInt("replicationFactor"));
		}

		if (config.hasPath("writePoolSize")) {
			store.setWritePoolSize(config.getInt("writePoolSize"));
		}

		if (config.hasPath("writeQueueSize")) {
			store.setWriteQueueSize(config.getInt("writeQueueSize"));
		}

		if (config.hasPath("readPoolSize")) {
			store.setReadPoolSize(config.getInt("readPoolSize"));
		}

		if (config.hasPath("readQueueSize")) {
			store.setReadQueueSize(config.getInt("readQueueSize"));
		}

		if (config.hasPath("readBatchSize")) {
			store.setReadBatchSize(config.getInt("readBatchSize"));
		}

		if (config.hasPath("lock-db")) {

			final String lockDb = config.getString("lock-db");
			final String lockTable = config.getString("lock-table");

			locks = new CassandraLockProvider(store, lockDb, Table.builder(lockTable)
					.rowKey(String.class)
					.columnKey(UUID.class)
					.defaultType(Long.class)
					.build());

		} else {

			locks = null;

		}

		store.connect();

	}

	public void deactivate() throws Exception {
		store.disconnect();
	}

	@Override
	public boolean has(final String keyspace) throws Exception {
		return store.has(keyspace);
	}

	@Override
	public void create(final String keyspace) throws Exception {
		store.create(keyspace);
	}

	@Override
	public void delete(final String keyspace) throws ConnectionException {
		store.delete(keyspace);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> boolean has(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {
		return store.has(keyspace, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void create(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {
		store.create(keyspace, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void update(final String keyspace,
			final Table<R, C, V> table)
			throws Exception {
		store.update(keyspace, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void truncate(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {
		store.truncate(keyspace, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void delete(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {
		store.delete(keyspace, table);
	}

	@Override
	public Batch batch(final String keyspace) throws Exception {
		return store.batch(keyspace);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> Observable<Boolean> exists(final String database,
			final Table<R, C, V> table, final R keys) throws Exception {
		return store.exists(database, table, keys);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableQueryBuilder<R, C> fetch(
			final String database,
			final Table<R, C, V> table, final R... keys) throws Exception {
		return store.fetch(database, table, keys);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableIndexQueryBuilder<R, C> query(
			final String database, final Table<R, C, V> table) throws Exception {
		return store.query(database, table);
	}

	@Override
	public Lock localLock(final String name, final long expiration, final TimeUnit units) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.localLock(name, expiration, units);
	}

	@Override
	public Lock localLock(final String name) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.localLock(name);
	}

	@Override
	public Lock globalLock(final String name, final long expiration, final TimeUnit units) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.globalLock(name, expiration, units);
	}

	@Override
	public Lock globalLock(final String name) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.globalLock(name);
	}

}
