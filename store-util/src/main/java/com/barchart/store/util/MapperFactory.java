package com.barchart.store.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapperFactory {

	private final ConcurrentMap<Class<?>, Object> instances = new ConcurrentHashMap<Class<?>, Object>();

	@SuppressWarnings("unchecked")
	public <M> M instance(final Class<M> cls) {

		if (!instances.containsKey(cls)) {
			try {
				final M inst = cls.getConstructor().newInstance();
				final Object existing = instances.putIfAbsent(cls, inst);
				return existing != null ? (M) existing : inst;
			} catch (final Exception e) {
				return null;
			}
		}

		return (M) instances.get(cls);

	}

}
