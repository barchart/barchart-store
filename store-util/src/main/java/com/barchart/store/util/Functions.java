package com.barchart.store.util;

import java.util.Comparator;
import java.util.TreeSet;

import rx.Observable;
import rx.util.functions.Func1;

public class Functions {

	public static <T> Func1<? super Iterable<T>, Observable<? extends T>> each() {

		return new Func1<Iterable<T>, Observable<? extends T>>() {

			@Override
			public Observable<T> call(final Iterable<T> iter) {
				return Observable.from(iter);
			}

		};

	}

	public static <T> Func1<? super Iterable<T>, Observable<? extends T>> sort() {
		return sort(null);
	}

	public static <T> Func1<? super Iterable<T>, Observable<? extends T>> sort(final Comparator<T> comparator) {

		return new Func1<Iterable<T>, Observable<? extends T>>() {

			@Override
			public Observable<T> call(final Iterable<T> iter) {
				final TreeSet<T> sorted = comparator != null ? new TreeSet<T>(comparator) : new TreeSet<T>();
				for (final T item : iter)
					sorted.add(item);
				return Observable.from(sorted);
			}

		};

	}

}
