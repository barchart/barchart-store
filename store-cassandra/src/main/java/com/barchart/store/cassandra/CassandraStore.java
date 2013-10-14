package com.barchart.store.cassandra;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func1;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;
import com.barchart.store.api.StoreService;
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
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

public class CassandraStore implements StoreService {

	private String[] seeds = new String[] {
			"eqx02.chicago.b.cassandra.eqx.barchart.com",
			"eqx01.chicago.b.cassandra.eqx.barchart.com",
			"aws01.us-east-1.b.cassandra.aws.barchart.com",
			"aws02.us-east-1.b.cassandra.aws.barchart.com" };

	private String clusterName = "b";

	private String strategyClass = "NetworkTopologyStrategy";
	private String[] zones = { "chicago", "us-east-1" };
	private int replicationFactor = 2;

	private String user = "cassandra";
	private String password = "cassandra";

	private final ExecutorService executor = Executors.newCachedThreadPool();

	static private AstyanaxContext<Cluster> clusterContext = null;

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

	public void setCredentials(final String user_, final String password_) {
		user = user_;
		password = password_;
	}

	public void connect() {

		final Builder builder =
				new AstyanaxContext.Builder()
						.forCluster(clusterName)
						.withAstyanaxConfiguration(

								new AstyanaxConfigurationImpl()
										.setDiscoveryType(
												NodeDiscoveryType.RING_DESCRIBE)
										// https://github.com/Netflix/astyanax/issues/127
										.setConnectionPoolType(
												ConnectionPoolType.ROUND_ROBIN)
										// https://github.com/Netflix/astyanax/issues/127
										.setCqlVersion("3.0.0")
										.setTargetCassandraVersion("1.2"))

						.withConnectionPoolConfiguration(
								new ConnectionPoolConfigurationImpl(
										"barchart_pool")
										.setSeeds(Joiner.on(",").join(seeds))
										.setMaxConns(100)
										.setMaxConnsPerHost(10)
										.setConnectTimeout(10000)
										.setSocketTimeout(10000)
										.setMaxTimeoutCount(10)

										// MJS: Added those to solidify the
										// connection as I get a timeout quite
										// often
										.setLatencyAwareUpdateInterval(10000)
										// Will resort hosts per token partition
										// every 10 seconds
										.setLatencyAwareResetInterval(10000)
										.setLatencyAwareBadnessThreshold(2)
										.setLatencyAwareWindowSize(100)
										// Uses last 100 latency samples. These
										// samples are in a FIFO q and will just
										// cycle themselves.
										.setAuthenticationCredentials(
												new SimpleAuthenticationCredentials(
														this.user,
														this.password)))
						.withConnectionPoolMonitor(
								new CountingConnectionPoolMonitor());

		// get cluster
		clusterContext =
				builder.buildCluster(ThriftFamilyFactory.getInstance());
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
	public <K, V> boolean has(final String keyspace, final Table<K, V> table)
			throws ConnectionException {
		return clusterContext.getClient().getKeyspace(keyspace)
				.describeKeyspace().getColumnFamily(table.name) != null;
	}

	@Override
	public <K, V> void create(final String keyspace, final Table<K, V> table)
			throws ConnectionException {
		clusterContext
				.getClient()
				.getKeyspace(keyspace)
				.createColumnFamily(
						new ColumnFamily<String, K>(table.name,
								StringSerializer.get(), // Row
								// key
								serializerFor(table.keyType)), // Column key
						getCFOptions(table));
	}

	@Override
	public void create(final String keyspace,
			final Table<String, String> table, final ColumnDef... columns)
			throws ConnectionException {
		clusterContext
				.getClient()
				.getKeyspace(keyspace)
				.createColumnFamily(
						new ColumnFamily<String, String>(table.name,
								StringSerializer.get(), // Row
								// key
								StringSerializer.get()), // Column key
						getCFOptions(table, columns));
	}

	@Override
	public <K, V> void update(final String keyspace, final Table<K, V> table)
			throws Exception {
		clusterContext
				.getClient()
				.getKeyspace(keyspace)
				.updateColumnFamily(
						new ColumnFamily<String, K>(table.name,
								StringSerializer.get(), // Row key
								serializerFor(table.keyType)), // Column key
						getCFOptions(table));
	}

	@Override
	public void update(final String keyspace,
			final Table<String, String> table, final ColumnDef... columns)
			throws Exception {
		clusterContext
				.getClient()
				.getKeyspace(keyspace)
				.updateColumnFamily(
						new ColumnFamily<String, String>(table.name,
								StringSerializer.get(), // Key Serializer
								StringSerializer.get()),// Column serializer
						getCFOptions(table, columns));
	}

	@Override
	public <K, V> void delete(final String keyspace, final Table<K, V> table)
			throws ConnectionException {
		clusterContext.getClient().getKeyspace(keyspace)
				.dropColumnFamily(table.name);
	}

	@SuppressWarnings("unchecked")
	private <T> Serializer<T> serializerFor(final Class<T> cls) {

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
			return (Serializer<T>) UUIDSerializer.get();
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
			return "DateTime";
		} else if (cls == Double.class) {
			return "DoubleType";
		} else if (cls == ByteBuffer.class) {
			return "BytesType";
		} else if (cls == byte[].class) {
			return "BytesType";
		} else if (cls == UUID.class) {
			return "UUIDType";
		}

		throw new IllegalArgumentException();

	}

	private <K, V> Map<String, Object> getCFOptions(final Table<K, V> table,
			final ColumnDef... columns) {

		final String keyValidator = validatorFor(table.keyType);

		final ImmutableMap.Builder<String, Object> builder =
				ImmutableMap
						.<String, Object> builder()
						.put("default_validation_class",
								validatorFor(table.defaultValueType))
						.put("key_validation_class", keyValidator)
						.put("comparator_type", keyValidator);

		if (columns != null && columns.length > 0) {

			final Map<String, Object> cols = new HashMap<String, Object>();

			for (final ColumnDef column : columns) {

				final Map<String, String> props = new HashMap<String, String>();
				props.put("validation_class", validatorFor(column.type()));

				if (column.isIndexed()) {
					props.put("index_name", column.key() + "_" + table.name
							+ "_idx");
					props.put("index_type", "KEYS");
				}

				cols.put(column.key(), props);

			}

			builder.put("column_metadata", cols);

		}

		return builder.build();

	}

	private <T> StoreRow<T> wrapRow(final Row<String, T> row) {
		return wrapColumns(row.getKey(), row.getColumns());
	}

	private <T> StoreRow<T> wrapColumns(final String key,
			final ColumnList<T> columns) {
		return new StoreRowImpl<T>(key, columns);
	}

	private class CassandraRowMutator<T> implements RowMutator<T> {

		private Integer ttl = null;
		private ColumnListMutation<T> clm = null;

		CassandraRowMutator(final ColumnListMutation<T> clm) {
			this.clm = clm;
		}

		@Override
		public RowMutator<T> set(final T column, final String value)
				throws Exception {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> set(final T column, final double value)
				throws Exception {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> set(final T column, final ByteBuffer value)
				throws Exception {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> set(final T column, final long value)
				throws Exception {
			clm.putColumn(column, value, ttl);
			return this;
		}

		@Override
		public RowMutator<T> ttl(final Integer ttl) throws Exception {
			this.ttl = ttl;
			return this;
		}

		@Override
		public void delete() throws Exception {
			clm.delete();
		}

		@Override
		public RowMutator<T> remove(final T column) throws Exception {
			clm.deleteColumn(column);
			return this;
		}

	}

	@Override
	public Batch batch(final String keyspace) throws Exception {
		return new CassandraBatchMutator(clusterContext.getClient()
				.getKeyspace(keyspace).prepareMutationBatch());
	}

	@Override
	public <K, V> ObservableQueryBuilder<K> fetch(final String database,
			final Table<K, V> table, final String... keys) throws Exception {
		if (keys == null || keys.length == 0) {
			return new CassandraAllRowsQuery<K>(database, table);
		} else if (keys.length == 1) {
			return new CassandraSingleRowQuery<K>(database, table, keys[0]);
		} else {
			return new CassandraMultiRowQuery<K>(database, table, keys);
		}
	}

	@Override
	public <K, V> ObservableQueryBuilder<K> query(final String database,
			final Table<K, V> table, final K column, final Object value)
			throws Exception {
		return new CassandraSearchQuery<K>(database, table, column, value);
	}

	private class CassandraBatchMutator implements Batch {

		private MutationBatch m = null;

		CassandraBatchMutator(final MutationBatch m) {
			this.m = m;
			this.m.setConsistencyLevel(ConsistencyLevel.CL_ONE);
		}

		@Override
		public <K, V> RowMutator<K> row(final Table<K, V> table,
				final String key) throws Exception {
			return new CassandraRowMutator<K>(m.withRow(
					new ColumnFamily<String, K>(table.name, StringSerializer
							.get(), serializerFor(table.keyType)), key));
		}

		@Override
		public void commit() throws Exception {
			m.execute();
		}

	}

	private static final class StoreRowImpl<T> implements StoreRow<T> {

		private final String key;
		private final ColumnList<T> columns;

		StoreRowImpl(final String key_, final ColumnList<T> columns_) {
			key = key_;
			columns = columns_;
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public Collection<T> columns() {
			return columns.getColumnNames();
		}

		@Override
		public StoreColumn<T> getByIndex(final int index) {
			final Column<T> c = columns.getColumnByIndex(index);
			if (c != null) {
				return new StoreColumnImpl<T>(c);
			}
			return null;
		}

		@Override
		public StoreColumn<T> get(final T name) {
			final Column<T> c = columns.getColumnByName(name);
			if (c != null) {
				return new StoreColumnImpl<T>(c);
			}
			return null;
		}

	}

	private static final class StoreColumnImpl<T> implements StoreColumn<T> {

		private final Column<T> column;

		StoreColumnImpl(final Column<T> column_) {
			column = column_;
		}

		@Override
		public T getName() {
			return column.getName();
		}

		@Override
		public String getString() throws Exception {
			return column.getStringValue();
		}

		@Override
		public Double getDouble() throws Exception {
			return column.getDoubleValue();
		}

		@Override
		public Integer getInt() throws Exception {
			return column.getIntegerValue();
		}

		@Override
		public Long getLong() throws Exception {
			return column.getLongValue();
		}

		@Override
		public Boolean getBoolean() throws Exception {
			return column.getBooleanValue();
		}

		@Override
		public Date getDate() throws Exception {
			return column.getDateValue();
		}

		@Override
		public ByteBuffer getBlob() throws Exception {
			return column.getByteBufferValue();
		}

		@Override
		public long getTimestamp() throws Exception {
			return column.getTimestamp();
		}
	}

	private abstract class CassandraBaseRowQuery<T> implements
			ObservableQueryBuilder<T> {

		protected final Keyspace keyspace;
		protected final ColumnFamilyQuery<String, T> query;

		protected RangeBuilder columnRange = null;
		protected T[] columns = null;

		private CassandraBaseRowQuery(final String database_,
				final Table<T, ?> table_) throws ConnectionException {

			keyspace = clusterContext.getClient().getKeyspace(database_);

			query =
					keyspace.prepareQuery(
							new ColumnFamily<String, T>(table_.name,
									StringSerializer.get(),
									serializerFor(table_.keyType)))
							.setConsistencyLevel(ConsistencyLevel.CL_TWO);

		}

		@Override
		public ObservableQueryBuilder<T> first(final int limit) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setLimit(limit);
			return this;
		}

		@Override
		public ObservableQueryBuilder<T> last(final int limit) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setReversed(true).setLimit(limit);
			return this;
		}

		@Override
		public ObservableQueryBuilder<T> start(final T column) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			if (column instanceof Boolean) {
				columnRange.setStart((Boolean) column);
			} else if (column instanceof byte[]) {
				columnRange.setStart((byte[]) column);
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
		public ObservableQueryBuilder<T> end(final T column) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			if (column instanceof Boolean) {
				columnRange.setEnd((Boolean) column);
			} else if (column instanceof byte[]) {
				columnRange.setEnd((byte[]) column);
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
		public ObservableQueryBuilder<T> prefix(final String prefix) {
			if (columnRange == null) {
				columnRange = new RangeBuilder();
			}
			columnRange.setStart(prefix + "\u00000").setEnd(prefix + "\uffff");
			return this;
		}

		@Override
		public ObservableQueryBuilder<T> columns(
				@SuppressWarnings("unchecked") final T... columns_) {
			columns = columns_;
			return this;
		}

	}

	private class CassandraSingleRowQuery<T> extends CassandraBaseRowQuery<T> {

		private final String key;

		private CassandraSingleRowQuery(final String database_,
				final Table<T, ?> table_, final String key_)
				throws ConnectionException {

			super(database_, table_);
			key = key_;

		}

		@Override
		public Observable<StoreRow<T>> build() {

			final RowQuery<String, T> rowQuery = query.getKey(key);

			if (columns != null) {
				rowQuery.withColumnSlice(columns);
			} else if (columnRange != null) {
				rowQuery.withColumnRange(columnRange.build());
			}

			return Observable
					.create(new Func1<Observer<StoreRow<T>>, Subscription>() {

						@Override
						public Subscription call(
								final Observer<StoreRow<T>> observer) {

							// Cassandra doesn't really do async
							// final
							// ListenableFuture<OperationResult<ColumnList<T>>>
							// future = rowQuery.executeAsync();

							executor.submit(new Runnable() {

								@Override
								public void run() {
									try {
										final OperationResult<ColumnList<T>> result =
												rowQuery.execute();
										final ColumnList<T> columns =
												result.getResult();
										observer.onNext(wrapColumns(key,
												columns));
										observer.onCompleted();
									} catch (final Exception e) {
										observer.onError(e);
									}
								}

							});

							return new Subscription() {
								@Override
								public void unsubscribe() {
									// No-op, single item query
								}
							};

						}

					});

		}

		@Override
		public Observable<StoreRow<T>> build(final int limit) {
			return build();
		}

		@Override
		public Observable<StoreRow<T>> build(final int limit,
				final int batchSize) {
			return build();
		}

	}

	private class CassandraMultiRowQuery<T> extends CassandraBaseRowQuery<T> {

		private final String[] keys;

		private CassandraMultiRowQuery(final String database_,
				final Table<T, ?> table_, final String... keys_)
				throws ConnectionException {

			super(database_, table_);
			keys = keys_;

		}

		@Override
		public Observable<StoreRow<T>> build() {
			return build(0);
		}

		@Override
		public Observable<StoreRow<T>> build(final int limit) {

			final RowSliceQuery<String, T> rowQuery = query.getKeySlice(keys);

			if (columns != null) {
				rowQuery.withColumnSlice(columns);
			} else if (columnRange != null) {
				rowQuery.withColumnRange(columnRange.build());
			}

			return Observable
					.create(new Func1<Observer<StoreRow<T>>, Subscription>() {

						private volatile boolean complete = false;

						@Override
						public Subscription call(
								final Observer<StoreRow<T>> observer) {

							executor.submit(new Runnable() {

								@Override
								public void run() {
									try {
										final OperationResult<Rows<String, T>> result =
												rowQuery.execute();
										int ct = 0;
										for (final Row<String, T> row : result
												.getResult()) {
											if (complete
													|| (limit > 0 && ct >= limit)) {
												break;
											}
											ct++;
											observer.onNext(wrapRow(row));
										}
										observer.onCompleted();
									} catch (final Exception e) {
										observer.onError(e);
									}
								}

							}, executor);

							return new Subscription() {
								@Override
								public void unsubscribe() {
									complete = true;
								}
							};

						}

					});

		}

		@Override
		public Observable<StoreRow<T>> build(final int limit,
				final int batchSize) {
			return build(limit);
		}

	}

	private class CassandraAllRowsQuery<T> extends CassandraBaseRowQuery<T> {

		private CassandraAllRowsQuery(final String database_,
				final Table<T, ?> table_) throws ConnectionException {

			super(database_, table_);

		}

		@Override
		public Observable<StoreRow<T>> build() {
			return build(0, 0);
		}

		@Override
		public Observable<StoreRow<T>> build(final int limit) {
			return build(limit, 0);
		}

		@Override
		public Observable<StoreRow<T>> build(final int limit,
				final int batchSize) {

			final AllRowsQuery<String, T> rowQuery = query.getAllRows();

			if (columns != null) {
				rowQuery.withColumnSlice(columns);
			} else if (columnRange != null) {
				rowQuery.withColumnRange(columnRange.build());
			}

			if (batchSize > 0) {
				rowQuery.setRowLimit(batchSize);
			}

			return Observable
					.create(new Func1<Observer<StoreRow<T>>, Subscription>() {

						private volatile boolean complete = false;

						@Override
						public Subscription call(
								final Observer<StoreRow<T>> observer) {

							executor.submit(new Runnable() {

								@Override
								public void run() {
									try {

										final OperationResult<Rows<String, T>> result =
												rowQuery.execute();

										int ct = 0;
										for (final Row<String, T> row : result
												.getResult()) {
											if (complete
													|| (limit > 0 && ct >= limit)) {
												break;
											}
											observer.onNext(wrapRow(row));
											ct++;
										}
										observer.onCompleted();
									} catch (final Exception e) {
										observer.onError(e);
									}
								}

							}, executor);

							return new Subscription() {
								@Override
								public void unsubscribe() {
									complete = true;
								}
							};

						}

					});

		}
	}

	private class CassandraSearchQuery<T> extends CassandraBaseRowQuery<T> {

		private final T column;
		private final Object value;

		private CassandraSearchQuery(final String database_,
				final Table<T, ?> table_, final T column_, final Object value_)
				throws ConnectionException {

			super(database_, table_);

			column = column_;
			value = value_;

		}

		@Override
		public Observable<StoreRow<T>> build() {
			return build(0, 0);
		}

		@Override
		public Observable<StoreRow<T>> build(final int limit) {
			return build(limit, 0);
		}

		@Override
		public Observable<StoreRow<T>> build(final int limit,
				final int batchSize) {

			IndexQuery<String, T> rowQuery = query.searchWithIndex();
			final IndexValueExpression<String, T> exp =
					rowQuery.addExpression().whereColumn(column).equals();
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

			if (columns != null) {
				rowQuery.withColumnSlice(columns);
			} else if (columnRange != null) {
				rowQuery.withColumnRange(columnRange.build());
			}

			if (batchSize > 0) {
				rowQuery.setRowLimit(batchSize);
			}

			final IndexQuery<String, T> indexQuery = rowQuery;

			return Observable
					.create(new Func1<Observer<StoreRow<T>>, Subscription>() {

						private volatile boolean complete = false;

						@Override
						public Subscription call(
								final Observer<StoreRow<T>> observer) {

							executor.submit(new Runnable() {

								@Override
								public void run() {
									try {

										final OperationResult<Rows<String, T>> result =
												indexQuery.execute();

										int ct = 0;
										for (final Row<String, T> row : result
												.getResult()) {
											if (complete
													|| (limit > 0 && ct >= limit)) {
												break;
											}
											observer.onNext(wrapRow(row));
											ct++;
										}
										observer.onCompleted();
									} catch (final Exception e) {
										observer.onError(e);
									}
								}

							}, executor);

							return new Subscription() {
								@Override
								public void unsubscribe() {
									complete = true;
								}
							};

						}

					});

		}
	}

}