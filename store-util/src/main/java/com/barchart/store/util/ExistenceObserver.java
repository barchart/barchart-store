package com.barchart.store.util;

import rx.Observer;

public abstract class ExistenceObserver<T> implements Observer<T> {

	private boolean exists = false;

	private final Observer<? super T> observer;

	public ExistenceObserver(final Observer<? super T> observer_) {
		observer = observer_;
	}

	public abstract void exists(Observer<? super T> observer);

	public abstract void missing(Observer<? super T> observer);

	@Override
	public void onNext(final T next) {
		exists = true;
	}

	@Override
	public void onError(final Throwable error) {
		observer.onError(error);
	}

	@Override
	public void onCompleted() {
		if (!exists) {
			try {
				missing(observer);
			} catch (final Throwable e) {
				observer.onError(e);
			}
		} else {
			exists(observer);
		}
	}

}