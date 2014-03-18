package com.barchart.store.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A data store table definition.
 *
 * @param <C> The column key data type
 * @param <V> The column value data type
 */
public class Table<R extends Comparable<R>, C extends Comparable<C>, V> {

	protected final String name;
	protected final Class<R> rowType;
	protected final Class<C> columnType;
	protected final Class<V> defaultValueType;

	protected final List<Column<C>> columns = new ArrayList<Column<C>>();

	protected Table(final String name_, final Class<R> rowType_, final Class<C> columnType_,
			final Class<V> defaultValueType_, final List<Column<C>> columns_) {

		name = name_;

		rowType = rowType_;
		columnType = columnType_;
		defaultValueType = defaultValueType_;

		if (columns_ != null)
			columns.addAll(columns_);

	}

	public String name() {
		return name;
	}

	public Class<R> rowType() {
		return rowType;
	}

	public Class<C> columnType() {
		return columnType;
	}

	public Class<V> defaultValueType() {
		return defaultValueType;
	}

	public List<Column<C>> columns() {
		return Collections.unmodifiableList(columns);
	}

	/**
	 * Create a new table definition builder. Default validator types are all
	 * String, which can be overridden by Builder methods.
	 */
	public static Builder<String, String, String> builder(final String name_) {
		return new Builder<String, String, String>(name_, String.class, String.class, String.class);
	}

	/**
	 * Create a new table definition with the specified key and value types.
	 *
	 * @deprecated Use builder() instead
	 */
	@Deprecated
	public static <C extends Comparable<C>, V> Table<String, C, V> make(final String name_,
			final Class<C> columnType_, final Class<V> valueType_) {
		return new Table<String, C, V>(name_, String.class, columnType_, valueType_, null);
	}

	/**
	 * Create a new table definition with the specified column key type, String
	 * row keys and String column values.
	 *
	 * @deprecated Use builder() instead
	 */
	@Deprecated
	public static <C extends Comparable<C>> Table<String, C, String> make(final String name_,
			final Class<C> columnType_) {
		return new Table<String, C, String>(name_, String.class, columnType_, String.class, null);
	}

	/**
	 * Create a new table definition with String row keys, column keys and
	 * values.
	 *
	 * @deprecated Use builder() instead
	 */
	@Deprecated
	public static Table<String, String, String> make(final String name_) {
		return builder(name_).build();
	}

	public static class Builder<R extends Comparable<R>, C extends Comparable<C>, V> {

		public final String name;
		public Class<R> rowType = null;
		public Class<C> columnType = null;
		public Class<V> defaultValueType = null;

		private final List<Column<C>> columns = new ArrayList<Column<C>>();

		private Builder(final String name_, final Class<R> rowType_, final Class<C> columnType_,
				final Class<V> defaultValueType_) {
			name = name_;
			rowType = rowType_;
			columnType = columnType_;
			defaultValueType = defaultValueType_;
		}

		public <T extends Comparable<T>> Builder<T, C, V> rowKey(final Class<T> type_) {
			return new Builder<T, C, V>(name, type_, columnType, defaultValueType);
		}

		public <T extends Comparable<T>> Builder<R, T, V> columnKey(final Class<T> type_) {
			return new Builder<R, T, V>(name, rowType, type_, defaultValueType);
		}

		public <T extends Comparable<T>> Builder<R, C, T> defaultType(final Class<T> type_) {
			return new Builder<R, C, T>(name, rowType, columnType, type_);
		}

		public Builder<R, C, V> column(final C key_, final Class<?> type_, final boolean indexed_) {
			columns.add(new ColumnImpl<C>(key_, type_, indexed_));
			return this;
		}

		public Table<R, C, V> build() {

			if (rowType == null)
				throw new IllegalArgumentException("Row key type not specified");

			if (columnType == null)
				throw new IllegalArgumentException("Column key type not specified");

			if (defaultValueType == null)
				throw new IllegalArgumentException("Default value type not specified");

			return new Table<R, C, V>(name, rowType, columnType, defaultValueType, columns);

		}

	}

	public interface Column<C> {

		/**
		 * Column key validator type
		 */
		C key();

		/**
		 * True if column has a secondary index
		 */
		boolean isIndexed();

		/**
		 * Column value validator type
		 */
		Class<?> type();

	}

	private static class ColumnImpl<C> implements Column<C> {

		private final C key;
		private final boolean indexed;
		private final Class<?> type;

		public ColumnImpl(final C key_, final Class<?> type_, final boolean indexed_) {
			key = key_;
			type = type_;
			indexed = indexed_;
		}

		@Override
		public C key() {
			return key;
		}

		@Override
		public boolean isIndexed() {
			return indexed;
		}

		@Override
		public Class<?> type() {
			return type;
		}

	}

}