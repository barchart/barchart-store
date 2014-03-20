package com.barchart.store.heap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;

public abstract class QueryBuilderBase<R extends Comparable<R>, C extends Comparable<C>> implements
		ObservableQueryBuilder<R, C> {

	protected NavigableSet<C> columns = null;
	protected C start = null;
	protected C end = null;
	protected int limit = 0;
	protected boolean reversed = false;
	protected String prefix = null;

	@Override
	public ObservableQueryBuilder<R, C> first(final int limit_) {
		limit = limit_;
		reversed = false;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> last(final int limit_) {
		limit = limit_;
		reversed = true;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> reverse(final boolean reversed_) {
		reversed = reversed_;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> start(final C column) {
		start = column;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> end(final C column) {
		end = column;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> prefix(final String prefix_) {
		prefix = prefix_;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> columns(final C... columns_) {
		columns = new TreeSet<C>(Arrays.asList(columns_));
		return this;
	}

	protected class RowFilter implements StoreRow<R, C> {

		private final HeapRow<R, C> row;
		private final List<C> visible;

		public RowFilter(final HeapRow<R, C> row_) {
			row = row_;
			visible = selectedColumns();
		}

		private List<C> selectedColumns() {

			final List<C> selected = new ArrayList<C>();

			NavigableSet<C> range = row.unsafeColumns();

			// Filter by ranges

			if (start != null && end != null) {
				range = range.subSet(start, true, end, true);
			} else if (start != null) {
				range = range.tailSet(start, true);
			} else if (end != null) {
				range = range.headSet(end, true);
			}

			if (columns != null && columns.size() > 0) {
				range = range.subSet(columns.first(), true, columns.last(), true);
			}

			// Reverse set if needed

			if (reversed) {
				range = range.descendingSet();
			}

			// Filter current range by column selector

			ColumnSelector<C> selector;
			if (columns != null && columns.size() > 0) {
				selector = new NameSelector<C>(columns);
			} else if (prefix != null) {
				selector = new PrefixSelector<C>(prefix);
			} else {
				selector = new AllSelector<C>();
			}

			// Limit number of columns returned

			int found = 0;
			final long now = System.currentTimeMillis();

			for (final C col : range) {

				// Filter by column selector
				if (selector.select(col)) {

					if (limit == 0 || found < limit) {

						final HeapColumn<C> hc = row.getImpl(col);

						// Check for column expiration
						if (hc.ttl() == 0 || hc.getTimestamp() + (hc.ttl() * 1000) > now) {

							selected.add(col);
							found++;

						} else {

							// Expired, remove
							row.delete(col);

						}

					}

				}

			}

			return selected;

		}

		@Override
		public R getKey() {
			return row.getKey();
		}

		@Override
		public Collection<C> columns() {
			return Collections.unmodifiableCollection(visible);
		}

		@Override
		public StoreColumn<C> getByIndex(final int index) {

			if (visible.size() > 0) {

				if (index > visible.size() - 1) {
					throw new ArrayIndexOutOfBoundsException();
				}

				return row.get(visible.get(index));

			} else {
				return row.getByIndex(index);
			}

		}

		@Override
		public StoreColumn<C> get(final C name) {
			if (visible.size() == 0 || visible.contains(name)) {
				return row.get(name);
			}
			return null;
		}

		@Override
		public int compareTo(final StoreRow<R, C> o) {
			return getKey().compareTo(o.getKey());
		}

	}

	private static interface ColumnSelector<C extends Comparable<C>> {

		boolean select(C name);

	}

	private static final class PrefixSelector<C extends Comparable<C>> implements ColumnSelector<C> {

		private final String prefix;

		PrefixSelector(final String prefix_) {
			prefix = prefix_;
		}

		@Override
		public boolean select(final C name) {
			return name.toString().startsWith(prefix);
		}

	}

	private static final class NameSelector<C extends Comparable<C>> implements ColumnSelector<C> {

		private final Collection<C> names;

		NameSelector(final Collection<C> names_) {
			names = names_;
		}

		@Override
		public boolean select(final C name) {
			return names.contains(name);
		}

	}

	private static final class AllSelector<C extends Comparable<C>> implements ColumnSelector<C> {

		@Override
		public boolean select(final C name) {
			return true;
		}

	}

}
