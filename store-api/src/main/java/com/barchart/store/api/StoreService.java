package com.barchart.store.api;

import rx.Observable;

/**
 * Data store interface for persisting key/value data of various types. Built to
 * support the typical features of modern distributed key/value stores
 * (Cassandra, Dynamo, Voldemort, Riak, etc).
 * 
 * Usage examples:
 * 
 * <pre>
 * // Create a database
 * store.create("database");
 * 
 * // Create a table
 * Table<String,String> test = Table.make("test");
 * store.create("database", test);
 * 
 * // Update some data
 * Batch batch = store.batch("database", test);
 * 
 * // Add or update a row
 * batch.row("new-row-key")
 *      .set("column1", "value1")
 *      .set("column2", "value2")
 *      .set("column3", 500);
 * 
 * // Remove a column from a row
 * batch.row("update-row-key").remove("column4");
 * 
 * // Delete a row
 * batch.row("key-to-delete").delete();
 * 
 * // Commit all batch operations
 * batch.commit();
 * 
 * // Fetch all columns in a row
 * store.fetch("database", test, "row-key").build().subscribe(observer);
 * 
 * // Fetch multiple rows
 * store.fetch("database", test, "row-key-1", "row-key-2", "row-key-3")
 *      .build().subscribe(observer);
 * 
 * // Fetch specific columns in a row
 * store.fetch("database", test, "row-key")
 *      .columns("column1", "column2")
 *      .build().subscribe(observer);
 * 
 * // Fetch last 5 columns in a row (useful with ordered wide rows)
 * store.fetch("database", test, "row-key")
 *      .last(5)
 *      .build().subscribe(observer);
 *      
 * // Query rows based on secondary indexes
 * store.query("database", test)
 *      .where("column1", "value1")
 *      .where("column3", 1000, Operator.LT)
 *      .build().subscribe(observer);
 * 
 * </pre>
 * 
 * @author jeremy
 */
// @ProviderType
public interface StoreService {

	// Database management

	/**
	 * Check if the named database exists.
	 */
	boolean has(String database) throws Exception;

	/**
	 * Create a new database.
	 */
	void create(String database) throws Exception;

	/**
	 * Delete the named database and all data in it.
	 */
	void delete(String database) throws Exception;

	// Table management

	/**
	 * Check if the specified table exists.
	 */
	<K, V> boolean has(String database, Table<K, V> table) throws Exception;

	/**
	 * Create the specified table with dynamic columns (wide rows).
	 */
	<K, V> void create(String database, Table<K, V> table) throws Exception;

	/**
	 * Create the specified table with a defined set of columns (useful for
	 * secondary indexes).
	 */
	void create(String database, Table<String, String> table,
			ColumnDef... columns) throws Exception;

	/**
	 * Update an existing table definition with dynamic columns (wide rows).
	 */
	<K, V> void update(String database, Table<K, V> table) throws Exception;

	/**
	 * Update an existing table definition with a defined set of columns.
	 */
	void update(String database, Table<String, String> table,
			ColumnDef... columns) throws Exception;

	/**
	 * Delete the specified table and all data in it.
	 */
	<K, V> void delete(String database, Table<K, V> table) throws Exception;

	// Batch row updates

	/**
	 * Start a batch update process for the specified database. The Batch object
	 * supports a fluent API for batching one or multiple row updates together
	 * into one query execution.
	 */
	Batch batch(String database) throws Exception;

	// Check for existence of a key

	/**
	 * Check if the specified row key exists in a table.
	 */
	<K, V> Observable<Boolean> exists(String database, Table<K, V> table,
			String key) throws Exception;

	// Fetch rows by key (or all keys)

	/**
	 * Fetch a set of rows by row key.
	 */
	<K, V> ObservableQueryBuilder<K> fetch(String database, Table<K, V> table,
			String... keys) throws Exception;

	// Search index for column value

	/**
	 * Query for rows from a secondary column value index. There must be at
	 * least one .where() clause used with this builder.
	 */
	<K, V> ObservableIndexQueryBuilder<K> query(String database,
			Table<K, V> table) throws Exception;

	// Table definition

	/**
	 * A data store table definition.
	 * 
	 * @param <K> The column key data type
	 * @param <V> The column value data type
	 */
	public static class Table<K, V> {

		public final String name;
		public final Class<K> keyType;
		public final Class<V> defaultValueType;

		private Table(final String name_, final Class<K> keyType_,
				final Class<V> defaultValueType_) {
			name = name_;
			keyType = keyType_;
			defaultValueType = defaultValueType_;
		}

		/**
		 * Create a new table definition with the specified column key and value
		 * types.
		 */
		public static <K, V> Table<K, V> make(final String name_,
				final Class<K> keyType_, final Class<V> valueType_) {
			return new Table<K, V>(name_, keyType_, valueType_);
		}

		/**
		 * Create a new table definition with the specified column key type and
		 * String column values.
		 */
		public static <K> Table<K, String> make(final String name_,
				final Class<K> keyType_) {
			return new Table<K, String>(name_, keyType_, String.class);
		}

		/**
		 * Create a new table definition with String column keys and values.
		 */
		public static Table<String, String> make(final String name_) {
			return new Table<String, String>(name_, String.class, String.class);
		}

	}

}
