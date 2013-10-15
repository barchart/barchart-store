package com.barchart.store.api;

import rx.Observable;

/**
 * Schema management:
 * 
 * if ( !service.has( keyspace ) ) service.createKeyspace( keyspace )
 * 
 * if ( !service.has( keyspace, table ) ) service.create( keyspace, table,
 * column info );
 * 
 * column info = StoreColumn array
 * 
 * Updates (multiple rows possible):
 * 
 * BatchMutator b = service.mutate( keyspace, table, primary key );
 * 
 * RowMutator m = b.row( table, key );
 * 
 * // delete rows and set columns in batch here m.delete(); m.set( "field",
 * field_value ); .set( "field2", field_value2 ); ...
 * 
 * b.commit();
 * 
 * Deletes:
 * 
 * service.mutate( keyspace, table, primary key ).delete(); service.delete(
 * keyspace, table ); service.delete( keyspace );
 * 
 * Reads:
 * 
 * 1) Always returns a list of store objects 2) By primary key is most efficient
 * 3) By field requires indexed fields
 */

// @ProviderType
public interface StoreService {

	// Database management

	boolean has(String database) throws Exception;

	void create(String database) throws Exception;

	void delete(String database) throws Exception;

	// Table management

	<K, V> boolean has(String database, Table<K, V> table) throws Exception;

	<K, V> void create(String database, Table<K, V> table) throws Exception;

	void create(String database, Table<String, String> table,
			ColumnDef... columns) throws Exception;

	<K, V> void update(String database, Table<K, V> table) throws Exception;

	void update(String database, Table<String, String> table,
			ColumnDef... columns) throws Exception;

	<K, V> void delete(String database, Table<K, V> table) throws Exception;

	// Batch row updates

	Batch batch(String database) throws Exception;

	// Check for existence of a key

	<K, V> Observable<Boolean> exists(String database, Table<K, V> table,
			String key) throws Exception;

	// Fetch rows by key (or all keys)

	<K, V> ObservableQueryBuilder<K> fetch(String database, Table<K, V> table,
			String... keys) throws Exception;

	// Search index for column value

	<K, V> ObservableIndexQueryBuilder<K> query(String database,
			Table<K, V> table) throws Exception;

	// Table definition

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

		public static <K, V> Table<K, V> make(final String name_,
				final Class<K> keyType_, final Class<V> valueType_) {
			return new Table<K, V>(name_, keyType_, valueType_);
		}

		public static <K> Table<K, String> make(final String name_,
				final Class<K> keyType_) {
			return new Table<K, String>(name_, keyType_, String.class);
		}

		public static Table<String, String> make(final String name_) {
			return new Table<String, String>(name_, String.class, String.class);
		}

	}

}
