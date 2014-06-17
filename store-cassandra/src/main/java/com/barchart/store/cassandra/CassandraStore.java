package com.barchart.store.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableIndexQueryBuilder.Operator;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.IndexOperationExpression;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.IndexValueExpression;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

public class CassandraStore implements StoreService {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private String[] seeds = new String[] {
			"eqx02.chicago.b.cassandra.eqx.barchart.com",
			"eqx01.chicago.b.cassandra.eqx.barchart.com",
			"aws01.us-east-1.b.cassandra.aws.barchart.com",
			"aws02.us-east-1.b.cassandra.aws.barchart.com"
	};

	private String clusterName = "b";

	private String strategyClass = "NetworkTopologyStrategy";
	private String[] zones = {
			"chicago", "us-east-1"
	};
	private int replicationFactor = 2;
	private ConsistencyLevel readConsistency = ConsistencyLevel.CL_LOCAL_QUORUM;
	private ConsistencyLevel writeConsistency = ConsistencyLevel.CL_LOCAL_QUORUM;

	private int maxConns = 100;
	private int maxConnsPerHost = 50;
	private int maxRowSlice = 100;

	private String user = "cassandra";
	private String password = "cassandra";

	private final ThreadPoolExecutor executor;
	private final BlockingQueue<Runnable> executorQueue;

	private final AtomicLong queryCount = new AtomicLong(1);
	private final AtomicInteger concurrentQueries = new AtomicInteger(0);

	private final ConcurrentMap<Table<?, ?, ?>, ColumnFamily<?, ?>> columnFamilies =
			new ConcurrentHashMap<Table<?, ?, ?>, ColumnFamily<?, ?>>();

	private AstyanaxContext<Cluster> clusterContext = null;

	public CassandraStore() {
		executorQueue = new LinkedBlockingQueue<Runnable>();
		executor = new ThreadPoolExecutor(maxConns, maxConns, 60, TimeUnit.SECONDS, executorQueue, new DaemonFactory());
	}

	public void setSeeds(final String... seeds_) {
		seeds = seeds_;
	}

	public void setCluster(final String name_) {
		clusterName = name_;
	}

	public void setStrategy(final String strategy_) {
		strategyClass = strategy_;
	}

	public void setZones(final String... zones_) {
		zones = zones_;
	}

	public void setReplicationFactor(final int factor_) {
		replicationFactor = factor_;
	}

	/**
	 * This should be tuned in conjunction with setMaxConnectionsPerHost() to
	 * ensure that the number of connections is not greater than (hosts x
	 * maxConnsPerHost) for your cluster. If it is, you may encounter pool
	 * timeouts during heavy use.
	 */
	public void setMaxConnections(final int max_) {
		if (max_ < maxConns) {
			executor.setCorePoolSize(max_);
			executor.setMaximumPoolSize(max_);
		} else {
			executor.setMaximumPoolSize(max_);
			executor.setCorePoolSize(max_);
		}
		maxConns = max_;
	}

	/**
	 * The maximum number of connections to open to each cluster node.
	 */
	public void setMaxConnectionsPerHost(final int max_) {
		maxConnsPerHost = max_;
	}

	/**
	 * The maximum number of rows to request by key per query. Requesting too
	 * many rows by key seems to send C* into a tailspin. If more than max_ keys
	 * are requested by the client, they will be split into multiple queries
	 * behind the scenes (but will look the same to the client.)
	 */
	public void setMaxRowSlice(final int max_) {
		maxRowSlice = max_;
	}

	public void setReadConsistency(final ConsistencyLevel level_) {
		readConsistency = level_;
	}

	public void setWriteConsistency(final ConsistencyLevel level_) {
		writeConsistency = level_;
	}

	public void setCredentials(final String user_, final String password_) {
		user = user_;
		password = password_;
	}

	public void connect() {

		final Builder builder = new AstyanaxContext.Builder()
				.forCluster(clusterName)
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
						.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
						// https://github.com/Netflix/astyanax/issues/127
						.setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
						// https://github.com/Netflix/astyanax/issues/127
						.setCqlVersion("3.0.0")
						.setTargetCassandraVersion("1.2"))
				.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(
						"barchart_pool")
						.setSeeds(Joiner.on(",").join(seeds))
						.setMaxConns(maxConns)
						.setMaxConnsPerHost(maxConnsPerHost)
						.setConnectTimeout(5000)
						.setSocketTimeout(10000)
						.setMaxTimeoutCount(10)
						// This is not a network timeout, but a connection pool
						// timeout if all connections are in use. In theory this
						// should not ever happen since our Executor has the
						// same number of threads as the connection pool, but it
						// still does.
						.setMaxTimeoutWhenExhausted(30000)
						// Added those to solidify the connection as I get a
						// timeout quite often
						.setLatencyAwareUpdateInterval(10000)
						// Resort hosts per token partition every 10 seconds
						.setLatencyAwareResetInterval(10000)
						.setLatencyAwareBadnessThreshold(2)
						// Uses last 100 latency samples
						.setLatencyAwareWindowSize(100)
						.setAuthenticationCredentials(new SimpleAuthenticationCredentials(this.user, this.password)))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

		// get cluster
		clusterContext = builder.buildCluster(ThriftFamilyFactory.getInstance());

		clusterContext.start();

	}

	public void disconnect() {

		if (clusterContext != null) {

			clusterContext.shutdown();
			clusterContext = null;
		}
	}

	@Override
	public boolean has(final String keyspace) throws Exception {
		return clusterContext.getClient().describeKeyspace(keyspace) != null;
	}

	@Override
	public void create(final String keyspace) throws Exception {
		final Map<String, String> options = new HashMap<String, String>();

		if (strategyClass.equals("SimpleStrategy")) {
			options.put("replication_factor", String.valueOf(replicationFactor));
		} else if (strategyClass.equals("NetworkTopologyStrategy")) {
			for (final String zone : zones) {
				options.put(zone, String.valueOf(replicationFactor));
			}
		} else {
			throw new Exception("Unsupported strategy used");
		}

		final KeyspaceDefinition keyspaceDef =
				clusterContext.getClient().makeKeyspaceDefinition();
		keyspaceDef.setName(keyspace);
		keyspaceDef.setStrategyClass(strategyClass);
		keyspaceDef.setStrategyOptions(options);

		clusterContext.getClient().addKeyspace(keyspaceDef);
	}

	@Override
	public void delete(final String keyspace) throws ConnectionException {
		clusterContext.getClient().dropKeyspace(keyspace);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> boolean has(final String keyspace,
			final Table<R, C, V> table) throws ConnectionException {
		return clusterContext.getClient().getKeyspace(keyspace)
				.describeKeyspace().getColumnFamily(table.name()) != null;
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void create(final String keyspace,
			final Table<R, C, V> table) throws ConnectionException {

		clusterContext.getClient().getKeyspace(keyspace)
				.createColumnFamily(columnFamily(table), getCFOptions(table));

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void update(final String keyspace,
			final Table<R, C, V> table) throws Exception {

		clusterContext.getClient().getKeyspace(keyspace)
				.updateColumnFamily(columnFamily(table), getCFOptions(table));

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void truncate(final String keyspace,
			final Table<R, C, V> table) throws ConnectionException {
		clusterContext.getClient().getKeyspace(keyspace).truncateColumnFamily(table.name());
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void delete(final String keyspace,
			final Table<R, C, V> table) throws ConnectionException {
		clusterContext.getClient().getKeyspace(keyspace)
				.dropColumnFamily(table.name());
	}

	@SuppressWarnings("unchecked")
	private static <T> Serializer<T> serializerFor(final Class<T> cls) {

		if (cls == String.class) {
			return (Serializer<T>) StringSerializer.get();
		} else if (cls == byte[].class) {
			return (Serializer<T>) BytesArraySerializer.get();
		} else if (cls == Boolean.class) {
			return (Serializer<T>) BooleanSerializer.get();
		} else if (cls == ByteBuffer.class) {
			return (Serializer<T>) ByteBufferSerializer.get();
		} else if (cls == Double.class) {
			return (Serializer<T>) DoubleSerializer.get();
		} else if (cls == Integer.class) {
			return (Serializer<T>) IntegerSerializer.get();
		} else if (cls == Long.class) {
			return (Serializer<T>) LongSerializer.get();
		} else if (cls == Date.class) {
			return (Serializer<T>) DateSerializer.get();
		} else if (cls == UUID.class) {
			return (Serializer<T>) TimeUUIDSerializer.get();
		}

		throw new IllegalArgumentException();

	}

	private <T> String validatorFor(final Class<T> cls) {

		if (cls == String.class) {
			return "UTF8Type";
		} else if (cls == Integer.class) {
			return "IntegerType";
		} else if (cls == Long.class) {
			return "LongType";
		} else if (cls == Boolean.class) {
			return "BooleanType";
		} else if (cls == Date.class) {
			return "DateType";
		} else if (cls == Double.class) {
			return "DoubleType";
		} else if (cls == ByteBuffer.class) {
			return "BytesType";
		} else if (cls == byte[].class) {
			return "BytesType";
		} else if (cls == UUID.class) {
			return "TimeUUIDType";
		}

		throw new IllegalArgumentException();

	}

	private <R extends Comparable<R>, C extends Comparable<C>, V> Map<String, Object> getCFOptions(
			final Table<R, C, V> table) {

		final String rowValidator = validatorFor(table.rowType());
		final String columnValidator = validatorFor(table.columnType());
		final String valueValidator = validatorFor(table.defaultValueType());

		final ImmutableMap.Builder<String, Object> builder =
				ImmutableMap.<String, Object> builder()
						.put("key_validation_class", rowValidator)
						.put("comparator_type", columnValidator)
						.put("default_validation_class", valueValidator);

		if (table.columns().size() > 0) {

			final Map<String, Object> cols = new HashMap<String, Object>();

			for (final Table.Column<C> column : table.columns()) {

				if (column.key() instanceof String) {

					final String columnName = (String) column.key();

					final Map<String, String> props = new HashMap<String, String>();
					props.put("validation_class", validatorFor(column.type()));

					if (column.isIndexed()) {
						props.put("index_name", safeIndexName(table.name(), columnName));
						props.put("index_type", "KEYS");
					}

					cols.put(columnName, props);

				} else {

					throw new IllegalArgumentException("Attempted to define an explicit column "
							+ "with a non-String key (not currently supported by Astyanax).");

				}

			}

			builder.put("column_metadata", cols);

		}

		return builder.build();

	}

	private static String safeIndexName(final String table, final String field) {
		return table.replaceAll("[-\\.]", "_") + "_"
				+ field.replaceAll("[-\\.]", "_") + "_idx";
	}

	private static <R extends Comparable<R>, C extends Comparable<C>> StoreRow<R, C> wrapRow(final Row<R, C> row) {
		return wrapColumns(row.getKey(), row.getColumns());
	}

	private static <R extends Comparable<R>, C extends Comparable<C>> StoreRow<R, C> wrapColumns(final R key,
			final ColumnList<C> columns) {
		return new StoreRowImpl<R, C>(key, columns);
	}

	private static class CassandraRowMutator<T> implements RowMutator<T> {

		private final CassandraBatchMutator batch;
		private final ColumnListMutation<T> clm;

		private Integer ttl = null;

		CassandraRowMutator(final CassandraBatchMutator batch_, final ColumnListMutation<T> clm_) {
			batch = batch_;
			clm = clm_;
		}

		@Override
		public RowMutator<T> set(final T column, final String value) {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> set(final T column, final double value) {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> set(final T column, final ByteBuffer value) {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> set(final T column, final int value) {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> set(final T column, final long value) {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> ttl(final Integer ttl) {
			this.ttl = ttl;
			return this;
		}

		@Override
		public CassandraBatchMutator delete() {
			clm.delete();
			return batch;
		}

		@Override
		public RowMutator<T> remove(final T column) {
			clm.deleteColumn(column);
			return this;
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

	}

	@Override
	public Batch batch(final String keyspace) throws Exception {
		return new CassandraBatchMutator(clusterContext.getClient().getKeyspace(keyspace));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> Observable<Boolean> exists(final String database,
			final Table<R, C, V> table, final R key) throws Exception {

		return fetch(database, table, key).build().exists(new Func1<StoreRow<R, C>, Boolean>() {

			@Override
			public Boolean call(final StoreRow<R, C> row) {
				return row.columns().size() > 0;
			}

		});

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableQueryBuilder<R, C> fetch(
			final String database, final Table<R, C, V> table, final R... keys) throws Exception {
		if (keys == null || keys.length == 0) {
			return new CassandraAllRowsQuery<R, C>(database, table,
					readConsistency, executor);
		} else if (keys.length == 1) {
			return new CassandraSingleRowQuery<R, C>(database, table,
					readConsistency, executor, keys[0]);
		} else {
			return new CassandraMultiRowQuery<R, C>(database, table,
					readConsistency, executor, keys);
		}
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableIndexQueryBuilder<R, C> query(
			final String database, final Table<R, C, V> table) throws Exception {
		return new CassandraSearchQuery<R, C>(database, table, readConsistency, executor);
	}

	@SuppressWarnings("unchecked")
	private <R extends Comparable<R>, C extends Comparable<C>> ColumnFamily<R, C> columnFamily(
			final Table<R, C, ?> table) {

		ColumnFamily<?, ?> cf = columnFamilies.get(table);

		if (cf == null) {

			cf = new ColumnFamily<R, C>(table.name(),
					serializerFor(table.rowType()),
					serializerFor(table.columnType()));

			columnFamilies.putIfAbsent(table, cf);

		}

		return (ColumnFamily<R, C>) cf;

	}

	private volatile long lastConnectionWarning = 0;

	private <R> long queryStart(final String label, final R... keys) {

		if (log.isDebugEnabled() && executorQueue.size() > 0) {
			final long now = System.currentTimeMillis();
			if (now - lastConnectionWarning > 10000) {
				lastConnectionWarning = now;
				log.debug("Executor pool is full (" + maxConns + " threads). While this is "
						+ "not necessarily a problem, you should verify pool configuration "
						+ "if latency becomes an issue.");
			}
		}

		if (log.isTraceEnabled()) {

			final long qct = queryCount.getAndIncrement();
			final int conc = concurrentQueries.incrementAndGet();

			log.trace("START " + qct + " (" + Thread.currentThread().getName() + ", " + conc + " executing): " + label
					+ "=[" + Joiner.on(", ").join(keys) + "]");

			return qct;

		}

		return 0;

	}

	private void queryFinish(final long query, final int rows) {

		if (log.isTraceEnabled()) {
			log.trace("FINISH " + query + " (" + Thread.currentThread().getName() + "): " + rows + " rows");
			concurrentQueries.decrementAndGet();
		}

	}

	private void queryFailed(final long query, final Exception e) {

		if (log.isTraceEnabled()) {
			log.trace("FAILED " + query + " (" + Thread.currentThread().getName() + ")", e);
			concurrentQueries.decrementAndGet();
		}

	}

	private class CassandraBatchMutator implements Batch {

		private MutationBatch batch = null;

		CassandraBatchMutator(final Keyspace keyspace) {
			batch = keyspace.prepareMutationBatch().withConsistencyLevel(writeConsistency);
		}

		@Override
		public <R extends Comparable<R>, C extends Comparable<C>, V> RowMutator<C> row(final Table<R, C, V> table,
				final R key) {
			return new CassandraRowMutator<C>(this, batch.withRow(columnFamily(table), key));
		}

		@Override
		public Observable<Boolean> commit() {

			// Create a cached Observable for one-time execution
			final Observable<Boolean> obs = Observable.create(new Observable.OnSubscribe<Boolean>() {

				@Override
				public void call(final Subscriber<? super Boolean> subscriber) {

					executor.execute(new Runnable() {

						@Override
						public void run() {

							final long qct = queryStart("commit");

							try {

								batch.execute();

								queryFinish(qct, 0);

								subscriber.onCompleted();

							} catch (final Exception e) {

								queryFailed(qct, e);

								subscriber.onError(e);

							}

						}

					});

				}

			}).cache();

			// Force subscription for immediate execution
			obs.subscribe();

			return obs;

		}

	}

	private static final class StoreRowImpl<R extends Comparable<R>, C extends Comparable<C>> implements StoreRow<R, C> {

		private final R key;
		private final ColumnList<C> columns;

		private List<C> names = null;

		StoreRowImpl(final R key_, final ColumnList<C> columns_) {

			key = key_;
			columns = columns_;

		}

		@Override
		public R getKey() {
			return key;
		}

		@Override
		public Collection<C> columns() {

			// Astyanax/Thrift just stores these as unordered HashMap keys,
			// but we want the order to match the indexing.

			// return columns.getColumnNames();

			if (names == null) {
				names = new ArrayList<C>();
				for (int i = 0; i < columns.size(); i++) {
					names.add(columns.getColumnByIndex(i).getName());
				}
			}

			return names;

		}

		@Override
		public StoreColumn<C> getByIndex(final int index) {
			final Column<C> c = columns.getColumnByIndex(index);
			if (c != null) {
				return new StoreColumnImpl<C>(c);
			}
			return null;
		}

		@Override
		public StoreColumn<C> get(final C name) {
			final Column<C> c = columns.getColumnByName(name);
			if (c != null) {
				return new StoreColumnImpl<C>(c);
			}
			return null;
		}

		@Override
		public int compareTo(final StoreRow<R, C> o) {
			return key.compareTo(o.getKey());
		}

		@Override
		public int size() {
			return columns.size();
		}

	}

	private static final class StoreColumnImpl<T extends Comparable<T>> implements StoreColumn<T> {

		private final Column<T> column;

		StoreColumnImpl(final Column<T> column_) {
			column = column_;
		}

		@Override
		public T getName() {
			return column.getName();
		}

		@Override
		public String getString() {
			if (!column.hasValue()) {
				return null;
			}
			return column.getStringValue();
		}

		@Override
		public Double getDouble() {
			if (!column.hasValue()) {
				return null;
			}
			return column.getDoubleValue();
		}

		@Override
		public Integer getInt() {
			if (!column.hasValue()) {
				return null;
			}
			return column.getIntegerValue();
		}

		@Override
		public Long getLong() {
			if (!column.hasValue()) {
				return null;
			}
			return column.getLongValue();
		}

		@Override
		public Boolean getBoolean() {
			if (!column.hasValue()) {
				return null;
			}
			return column.getBooleanValue();
		}

		@Override
		public Date getDate() {
			if (!column.hasValue()) {
				return null;
			}
			return column.getDateValue();
		}

		@Override
		public ByteBuffer getBlob() {
			if (!column.hasValue()) {
				return null;
			}
			return column.getByteBufferValue();
		}

		@Override
		public long getTimestamp() {
			return column.getTimestamp();
		}

		@Override
		public int compareTo(final StoreColumn<T> that) {
			return getName().compareTo(that.getName());
		}
	}

	private abstract class CassandraBaseRowQuery<R extends Comparable<R>, C extends Comparable<C>> implements
			ObservableQueryBuilder<R, C> {

		protected final Keyspace keyspace;
		protected final ColumnFamilyQuery<R, C> query;
		protected final Executor executor;

		protected RangeBuilder columnRange = null;
		protected C[] columns = null;

		private CassandraBaseRowQuery(final String database_,
				final Table<R, C, ?> table_, final ConsistencyLevel level_,
				final Executor executor_) throws ConnectionException {

			keyspace = clusterContext.getClient().getKeyspace(database_);

			query = keyspace.prepareQuery(columnFamily(table_)).setConsistencyLevel(level_);

			executor = executor_;

		}

		@Override
		public ObservableQueryBuilder<R, C> first(final int limit) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setReversed(false).setLimit(limit);
			return this;
		}

		@Override
		public ObservableQueryBuilder<R, C> last(final int limit) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setReversed(true).setLimit(limit);
			return this;
		}

		@Override
		public ObservableQueryBuilder<R, C> limit(final int limit) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setLimit(limit);
			return this;
		}

		@Override
		public ObservableQueryBuilder<R, C> reverse(final boolean reversed_) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setReversed(reversed_);
			return this;
		}

		@Override
		public ObservableQueryBuilder<R, C> start(final C column) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			if (column instanceof Boolean) {
				columnRange.setStart((Boolean) column);
			} else if (column instanceof ByteBuffer) {
				columnRange.setStart((ByteBuffer) column);
			} else if (column instanceof Integer) {
				columnRange.setStart((Integer) column);
			} else if (column instanceof Double) {
				columnRange.setStart((Double) column);
			} else if (column instanceof Long) {
				columnRange.setStart((Long) column);
			} else if (column instanceof Date) {
				columnRange.setStart((Date) column);
			} else if (column instanceof String) {
				columnRange.setStart((String) column);
			} else if (column instanceof UUID) {
				columnRange.setStart((UUID) column);
			} else {
				throw new IllegalArgumentException("Invalid type");
			}
			return this;
		}

		@Override
		public ObservableQueryBuilder<R, C> end(final C column) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			if (column instanceof Boolean) {
				columnRange.setEnd((Boolean) column);
			} else if (column instanceof ByteBuffer) {
				columnRange.setEnd((ByteBuffer) column);
			} else if (column instanceof Integer) {
				columnRange.setEnd((Integer) column);
			} else if (column instanceof Double) {
				columnRange.setEnd((Double) column);
			} else if (column instanceof Long) {
				columnRange.setEnd((Long) column);
			} else if (column instanceof Date) {
				columnRange.setEnd((Date) column);
			} else if (column instanceof String) {
				columnRange.setEnd((String) column);
			} else if (column instanceof UUID) {
				columnRange.setEnd((UUID) column);
			} else {
				throw new IllegalArgumentException("Invalid type");
			}
			return this;
		}

		@Override
		public ObservableQueryBuilder<R, C> prefix(final String prefix) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setStart(prefix + "\u00000").setEnd(prefix + "\uffff");
			return this;
		}

		@Override
		public ObservableQueryBuilder<R, C> columns(final C... columns_) {
			columns = columns_;
			return this;
		}

	}

	private class CassandraSingleRowQuery<R extends Comparable<R>, C extends Comparable<C>> extends
			CassandraBaseRowQuery<R, C> {

		private final R key;

		private CassandraSingleRowQuery(final String database_,
				final Table<R, C, ?> table_, final ConsistencyLevel level_,
				final Executor executor_, final R key_)
				throws ConnectionException {

			super(database_, table_, level_, executor_);
			key = key_;

		}

		@Override
		public Observable<StoreRow<R, C>> build() {

			final RowQuery<R, C> rowQuery = query.getKey(key);

			if (columns != null) {
				rowQuery.withColumnSlice(columns);
			} else if (columnRange != null) {
				rowQuery.withColumnRange(columnRange.build());
			}

			return Observable.create(new Observable.OnSubscribe<StoreRow<R, C>>() {

				@Override
				public void call(final Subscriber<? super StoreRow<R, C>> subscriber) {

					executor.execute(new Runnable() {

						@Override
						public void run() {

							long qct = 0;

							try {

								qct = queryStart("keys");

								final OperationResult<ColumnList<C>> result = rowQuery.execute();

								queryFinish(qct, result.getResult().size());

								final ColumnList<C> columns = result.getResult();
								subscriber.onNext(wrapColumns(key, columns));

								subscriber.onCompleted();

							} catch (final Exception e) {

								queryFailed(qct, e);

								subscriber.onError(e);

							}
						}

					});

				}

			});

		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit) {
			return build();
		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit, final int batchSize) {
			return build();
		}

	}

	private class CassandraMultiRowQuery<R extends Comparable<R>, C extends Comparable<C>> extends
			CassandraBaseRowQuery<R, C> {

		private final R[] keys;

		private CassandraMultiRowQuery(final String database_,
				final Table<R, C, ?> table_, final ConsistencyLevel level_,
				final Executor executor_, final R... keys_)
				throws ConnectionException {

			super(database_, table_, level_, executor_);
			keys = keys_;

		}

		@Override
		public Observable<StoreRow<R, C>> build() {
			return build(0);
		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit) {
			return build(limit, 0);
		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit, final int batchSize) {

			return Observable.create(new Observable.OnSubscribe<StoreRow<R, C>>() {

				@Override
				public void call(final Subscriber<? super StoreRow<R, C>> subscriber) {

					executor.execute(new Runnable() {

						@Override
						public void run() {

							long qct = 0;

							try {

								int ct = 0;

								final int realBatchSize =
										batchSize == 0 ? maxRowSlice : Math.min(batchSize, maxRowSlice);

								outer:
								for (final R[] batch : batches(keys, realBatchSize)) {

									final RowSliceQuery<R, C> rowQuery = query.getKeySlice(batch);

									if (columns != null) {
										rowQuery.withColumnSlice(columns);
									} else if (columnRange != null) {
										rowQuery.withColumnRange(columnRange.build());
									}

									qct = queryStart("keys");

									final OperationResult<Rows<R, C>> result = rowQuery.execute();

									queryFinish(qct, result.getResult().size());

									for (final Row<R, C> row : result.getResult()) {

										if (subscriber.isUnsubscribed()) {
											return;
										}

										if (limit > 0 && ct >= limit) {
											break outer;
										}

										subscriber.onNext(wrapRow(row));
										ct++;

									}

								}

								subscriber.onCompleted();

							} catch (final Exception e) {

								queryFailed(qct, e);

								subscriber.onError(e);

							}

						}

					});

				}

			});

		}

		/**
		 * Slice a key set into multiple batches.
		 */
		private <T> List<T[]> batches(final T[] keys, final int batchSize) {

			final ArrayList<T[]> batches = new ArrayList<T[]>();

			if (batchSize == 0 || keys.length <= batchSize) {
				batches.add(keys);
			} else {

				int idx = 0;

				while (idx <= keys.length) {
					final int end = idx + batchSize > keys.length ? keys.length : idx + batchSize;
					batches.add(Arrays.copyOfRange(keys, idx, end));
					idx += batchSize;
				}

			}

			return batches;

		}

	}

	private class CassandraAllRowsQuery<R extends Comparable<R>, C extends Comparable<C>> extends
			CassandraBaseRowQuery<R, C> {

		private CassandraAllRowsQuery(final String database_,
				final Table<R, C, ?> table_, final ConsistencyLevel level_,
				final Executor executor_) throws ConnectionException {

			super(database_, table_, level_, executor_);

		}

		@Override
		public Observable<StoreRow<R, C>> build() {
			return build(0, 0);
		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit) {
			return build(limit, 0);
		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit, final int batchSize) {

			final AllRowsQuery<R, C> rowQuery = query.getAllRows();

			if (columns != null) {
				rowQuery.withColumnSlice(columns);
			} else if (columnRange != null) {
				rowQuery.withColumnRange(columnRange.build());
			}

			if (batchSize > 0) {
				rowQuery.setRowLimit(batchSize);
			}

			return Observable.create(new Observable.OnSubscribe<StoreRow<R, C>>() {

				@Override
				public void call(final Subscriber<? super StoreRow<R, C>> subscriber) {

					executor.execute(new Runnable() {

						@Override
						public void run() {

							long qct = 0;

							try {

								qct = queryStart("keys");

								final OperationResult<Rows<R, C>> result = rowQuery.execute();

								queryFinish(qct, 0);

								int ct = 0;
								for (final Row<R, C> row : result.getResult()) {

									if (subscriber.isUnsubscribed()) {
										return;
									}

									if (limit > 0 && ct >= limit) {
										break;
									}

									subscriber.onNext(wrapRow(row));
									ct++;

								}

								subscriber.onCompleted();

							} catch (final Exception e) {

								queryFailed(qct, e);

								subscriber.onError(e);

							}

						}

					});

				}

			});

		}
	}

	private static class ValueCompare {
		public Operator operator;
		public Object value;

		public ValueCompare(final Operator operator_, final Object value_) {
			operator = operator_;
			value = value_;
		}
	}

	private class CassandraSearchQuery<R extends Comparable<R>, C extends Comparable<C>> extends
			CassandraBaseRowQuery<R, C> implements ObservableIndexQueryBuilder<R, C> {

		Map<C, List<ValueCompare>> filters;

		private CassandraSearchQuery(final String database_, final Table<R, C, ?> table_,
				final ConsistencyLevel level_, final Executor executor_) throws ConnectionException {
			super(database_, table_, level_, executor_);
			filters = new HashMap<C, List<ValueCompare>>();
		}

		@Override
		public ObservableIndexQueryBuilder<R, C> where(final C column, final Object value) {
			return where(column, value, Operator.EQUAL);
		}

		@Override
		public ObservableIndexQueryBuilder<R, C> where(final C column, final Object value, final Operator operator) {
			List<ValueCompare> list = filters.get(column);
			if (list == null) {
				list = new ArrayList<ValueCompare>();
				filters.put(column, list);
			}
			list.add(new ValueCompare(operator, value));
			return this;
		}

		@Override
		public Observable<StoreRow<R, C>> build() {
			return build(0, 0);
		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit) {
			return build(limit, 0);
		}

		@Override
		public Observable<StoreRow<R, C>> build(final int limit, final int batchSize) {

			IndexQuery<R, C> rowQuery = query.searchWithIndex();

			boolean validQuery = filters.size() > 0 ? false : true;
			outerloop: for (final Map.Entry<C, List<ValueCompare>> entry : filters.entrySet()) {
				// Cassandra secondary index queries require at least one EQUAL
				// clause to run for some reason
				for (final ValueCompare vc : entry.getValue()) {
					if (vc.operator == Operator.EQUAL) {
						validQuery = true;
						break outerloop;
					}
				}
			}

			if (!validQuery) {
				throw new IllegalArgumentException("Secondary index queries require at least one EQUAL term");
			}

			for (final Map.Entry<C, List<ValueCompare>> entry : filters.entrySet()) {

				for (final ValueCompare vc : entry.getValue()) {

					final IndexOperationExpression<R, C> ops = rowQuery.addExpression().whereColumn(entry.getKey());
					final IndexValueExpression<R, C> exp;

					switch (vc.operator) {
						case GT:
							exp = ops.greaterThan();
							break;
						case GTE:
							exp = ops.greaterThanEquals();
							break;
						case LT:
							exp = ops.lessThan();
							break;
						case LTE:
							exp = ops.lessThanEquals();
							break;
						case EQUAL:
						default:
							exp = ops.equals();
					}

					final Object value = vc.value;

					if (value instanceof Boolean) {
						rowQuery = exp.value((Boolean) value);
					} else if (value instanceof byte[]) {
						rowQuery = exp.value((byte[]) value);
					} else if (value instanceof Integer) {
						rowQuery = exp.value((Integer) value);
					} else if (value instanceof Double) {
						rowQuery = exp.value((Double) value);
					} else if (value instanceof Long) {
						rowQuery = exp.value((Long) value);
					} else if (value instanceof String) {
						rowQuery = exp.value((String) value);
					}

				}

			}

			if (columns != null) {
				rowQuery.withColumnSlice(columns);
			} else if (columnRange != null) {
				rowQuery.withColumnRange(columnRange.build());
			}

			if (batchSize > 0) {
				rowQuery.setRowLimit(batchSize);
			}

			final IndexQuery<R, C> indexQuery = rowQuery;

			return Observable.create(new Observable.OnSubscribe<StoreRow<R, C>>() {

				@Override
				public void call(final Subscriber<? super StoreRow<R, C>> subscriber) {

					executor.execute(new Runnable() {

						@Override
						public void run() {

							long qct = 0;

							try {

								qct = queryStart("search");

								final OperationResult<Rows<R, C>> result = indexQuery.execute();

								queryFinish(qct, result.getResult().size());

								int ct = 0;

								for (final Row<R, C> row : result.getResult()) {

									if (subscriber.isUnsubscribed()) {
										return;
									}

									if (limit > 0 && ct >= limit) {
										break;
									}

									subscriber.onNext(wrapRow(row));
									ct++;

								}

								subscriber.onCompleted();

							} catch (final Exception e) {

								queryFailed(qct, e);

								subscriber.onError(e);

							}

						}

					});

				}

			});

		}
	}

	private static class DaemonFactory implements ThreadFactory {

		private static final AtomicInteger threadNumber = new AtomicInteger(1);

		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "cassandra-query-" + threadNumber.getAndIncrement());
			t.setDaemon(true);
			return t;
		}

	}

}
