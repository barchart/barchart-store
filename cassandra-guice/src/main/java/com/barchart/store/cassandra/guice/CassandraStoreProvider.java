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

@Component(CassandraStoreProvider.TYPE)
public class CassandraStoreProvider implements StoreService, StoreLockService {

	public static final String TYPE = "com.barchart.store.cassandra";

	private CassandraStore store;
	private CassandraStore copy;
	private double copyReadPercent = 0.0d;
	private CassandraLockProvider locks;

	@Inject
	@Named("#")
	private Config config;

	public CassandraStoreProvider() {
	}

	@Activate
	public void activate() {

		store = connect(config, config);

		if (config.hasPath("migration")) {

			final Config migrationConfig = config.getConfig("migration");

			copy = connect(config.getConfig("migration"), config);

			if (migrationConfig.hasPath("read-percent")) {
				copyReadPercent = migrationConfig.getDouble("read-percent");
			}

		} else {
			copy = null;
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

	}

	private CassandraStore connect(final Config cluster_, final Config common_) {

		final CassandraStore store_ = new CassandraStore();

		if (cluster_.hasPath("seeds")) {
			store_.setSeeds(cluster_.getStringList("seeds").toArray(new String[] {}));
		}

		if (cluster_.hasPath("cluster")) {
			store_.setCluster(cluster_.getString("cluster"));
		}

		if (cluster_.hasPath("username")) {
			store_.setCredentials(cluster_.getString("username"), config.getString("pass"));
		}

		if (cluster_.hasPath("strategyClass")) {
			store_.setStrategy(cluster_.getString("strategyClass"));
		}

		if (cluster_.hasPath("zones")) {

			final List<String> zonesList = cluster_.getStringList("zones");

			if (zonesList.size() > 0) {
				store_.setZones(zonesList.toArray(new String[] {}));
			}

		}

		if (cluster_.hasPath("replicationFactor")) {
			store_.setReplicationFactor(cluster_.getInt("replicationFactor"));
		}

		if (common_.hasPath("writePoolSize")) {
			store_.setWritePoolSize(common_.getInt("writePoolSize"));
		}

		if (common_.hasPath("writeQueueSize")) {
			store_.setWriteQueueSize(common_.getInt("writeQueueSize"));
		}

		if (common_.hasPath("readPoolSize")) {
			store_.setReadPoolSize(common_.getInt("readPoolSize"));
		}

		if (common_.hasPath("readQueueSize")) {
			store_.setReadQueueSize(common_.getInt("readQueueSize"));
		}

		if (common_.hasPath("readBatchSize")) {
			store_.setReadBatchSize(common_.getInt("readBatchSize"));
		}

		store_.connect();

		return store_;

	}

	public void deactivate() throws Exception {

		store.disconnect();

		if (copy != null) {
			copy.disconnect();
		}

	}

	private CassandraStore reader() {

		if (copy != null && copyReadPercent > 0 && Math.random() <= copyReadPercent) {
			return copy;
		}

		return store;

	}

	@Override
	public boolean has(final String keyspace) throws Exception {
		return reader().has(keyspace);
	}

	@Override
	public void create(final String keyspace) throws Exception {

		store.create(keyspace);

		if (copy != null) {
			copy.create(keyspace);
		}

	}

	@Override
	public void delete(final String keyspace) throws ConnectionException {

		store.delete(keyspace);

		if (copy != null) {
			copy.delete(keyspace);
		}

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> boolean has(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {
		return reader().has(keyspace, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void create(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {

		store.create(keyspace, table);

		if (copy != null) {
			copy.create(keyspace, table);
		}

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void update(final String keyspace,
			final Table<R, C, V> table)
			throws Exception {

		store.update(keyspace, table);

		if (copy != null) {
			copy.update(keyspace, table);
		}

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void truncate(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {

		store.truncate(keyspace, table);

		if (copy != null) {
			copy.truncate(keyspace, table);
		}

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void delete(final String keyspace,
			final Table<R, C, V> table)
			throws ConnectionException {

		store.delete(keyspace, table);

		if (copy != null) {
			copy.delete(keyspace, table);
		}

	}

	@Override
	public Batch batch(final String keyspace) throws Exception {

		if (copy != null) {
			return new MigrationBatch(store.batch(keyspace), copy.batch(keyspace));
		}

		return store.batch(keyspace);

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> Observable<Boolean> exists(final String database,
			final Table<R, C, V> table, final R keys) throws Exception {
		return reader().exists(database, table, keys);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableQueryBuilder<R, C> fetch(
			final String database,
			final Table<R, C, V> table, final R... keys) throws Exception {
		return reader().fetch(database, table, keys);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableIndexQueryBuilder<R, C> query(
			final String database, final Table<R, C, V> table) throws Exception {
		return reader().query(database, table);
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
