package com.barchart.store.util;

import java.util.ArrayList;
import java.util.List;

import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.StoreService.Table;

/**
 * Base class for defining store schemas.
 */
public abstract class StoreSchema {

	/**
	 * Override in subclass to provide table definitions for the update process.
	 */
	protected abstract SchemaTable<?, ?>[] tables();

	@SuppressWarnings("unchecked")
	public void update(final StoreService store, final String database)
			throws Exception {

		if (!store.has(database)) {
			store.create(database);
		}

		final SchemaTable<?, ?>[] tables = tables();

		for (final SchemaTable<?, ?> table : tables) {
			if (!store.has(database, table.table)) {
				if (table.columns.size() > 0) {
					store.create(database, (Table<String, String>) table.table,
							table.columns.toArray(new ColumnDef[] {}));
				} else {
					store.create(database, table.table);
				}
			} else {
				if (table.columns.size() > 0) {
					store.update(database, (Table<String, String>) table.table,
							table.columns.toArray(new ColumnDef[] {}));
				} else {
					store.update(database, table.table);
				}
			}
		}

	}

	/**
	 * Schema table definition.
	 * 
	 * @param <K> The default column key type.
	 * @param <V> The default column value type.
	 */
	protected static class SchemaTable<K, V> {

		private final Table<K, V> table;
		private final List<ColumnDef> columns = new ArrayList<ColumnDef>();

		public SchemaTable(final Table<K, V> table_) {
			table = table_;
		}

		public SchemaTable<K, V> index(final String key_, final Class<?> type_) {
			columns.add(new SchemaColumn(key_, type_));
			return this;
		}

	}

	private static class SchemaColumn implements ColumnDef {

		private final String key;
		private final Class<?> type;

		public SchemaColumn(final String key_, final Class<?> type_) {
			key = key_;
			type = type_;
		}

		@Override
		public String key() {
			return key;
		}

		@Override
		public boolean isIndexed() {
			return true;
		}

		@Override
		public Class<?> type() {
			return type;
		}

	}

}
