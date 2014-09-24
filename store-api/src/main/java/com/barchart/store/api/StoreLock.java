package com.barchart.store.api;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a distributed lock on top of a store service like Cassandra. This is a convenient locking mechanism
 * for applications that are already using a distributed store, but it does not scale efficiently compare to the
 * alternatives. Compared to solutions like Zookeeper or Hazelcast which proactively notify clients, acquiring a lock
 * requires constant polling if the initial lock acquisition fails, so it should not be used for high-contention locking
 * scenarios to avoid DDOS-ing the cluster with lock clients. It is best used as a coordination safety net for
 * low-traffic locking tasks.
 *
 * The locking table used by the row() and fetch() method implementations should use wide rows with the following data
 * validators in Cassandra:
 *
 * row key = String (lock name)
 *
 * column name = UUID (unique lock ID)
 *
 * column value = Long (lock expiration time)
 */
public abstract class StoreLock implements Lock {

	private static final Logger LOG = LoggerFactory.getLogger(StoreLock.class);

	private final UUID id;
	private final String name;
	private final long expiration;
	private final TimeUnit units;
	private final int interval;

	protected StoreLock(final UUID id_, final String name_, final long expiration_, final TimeUnit units_) {
		this(id_, name_, expiration_, units_, -1);
	}

	protected StoreLock(final UUID id_, final String name_, final long expiration_, final TimeUnit units_,
			final int checkInterval_) {
		id = id_;
		name = name_;
		expiration = expiration_;
		units = units_;
		interval = checkInterval_;
	}

	protected abstract RowMutator<UUID> row(String name) throws Exception;

	protected abstract ObservableQueryBuilder<String, UUID> fetch(String name) throws Exception;

	@Override
	public void lock() {

		while (true) {
			try {
				lockInterruptibly();
				return;
			} catch (final InterruptedException e) {
				// Reset state
				Thread.interrupted();
			} catch (final RuntimeException re) {
				unlock();
				throw re;
			} catch (final Throwable t) {
				unlock();
				throw new RuntimeException(t);
			}
		}

	}

	@Override
	public void lockInterruptibly() throws InterruptedException {

		register(60000);

		try {

			while (true) {

				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException();
				}

				try {

					if (tryAcquire()) {
						return;
					}

				} catch (final LockExpiredException le) {

					// Re-register lock request for another minute
					register(60000);

				} catch (final Throwable t) {
					LOG.error("Error attempting to acquire lock " + name + ", retrying", t);
				}

				if (interval > -1) {
					Thread.sleep(interval);
				}

			}

		} catch (final InterruptedException ie) {
			unlock();
			throw ie;
		} catch (final RuntimeException re) {
			unlock();
			throw re;
		} catch (final Throwable t) {
			unlock();
			throw new RuntimeException(t);
		}

	}

	@Override
	public boolean tryLock() {

		try {
			register(60000);
			return tryAcquire();
		} catch (final RuntimeException re) {
			unlock();
			throw re;
		} catch (final Throwable t) {
			unlock();
			throw new RuntimeException(t);
		}

	}

	@Override
	public boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException {

		final long ttl = TimeUnit.MILLISECONDS.convert(time, unit);

		try {

			register(ttl);

			while (true) {

				if (tryAcquire()) {
					return true;
				}

				if (interval > -1) {
					Thread.sleep(interval);
				}

			}

		} catch (final LockExpiredException le) {
			return false;

		} catch (final RuntimeException re) {
			unlock();
			throw re;
		} catch (final Throwable t) {
			unlock();
			throw new RuntimeException(t);
		}

	}

	@Override
	public void unlock() {
		cleanup(id);
	}

	private void register(final long ttl) {
		try {
			row(name).set(id, System.currentTimeMillis() + ttl).commit().toBlockingObservable().lastOrDefault(false);
		} catch (final Exception e) {
			LOG.error("Failed to register lock", e);
			unlock();
			throw new RuntimeException(e);
		}
	}

	private void cleanup(final UUID... ids) {

		try {

			final RowMutator<UUID> row = row(name);

			for (final UUID id : ids) {
				row.remove(id);
			}

			row.commit().toBlockingObservable().lastOrDefault(false);

			LOG.trace("unlocked: " + id);

		} catch (final Exception e) {

			LOG.error("Failed to release lock", e);
			throw new RuntimeException(e);

		}

	}

	private boolean tryAcquire() throws LockExpiredException {

		final List<UUID> expired = new ArrayList<UUID>();

		try {

			final StoreRow<String, UUID> row = fetch(name).build().toBlockingObservable().single();

			LOG.trace("lock id: " + id);
			LOG.trace("lock queue: " + row.size());
			for (int i = 0; i < row.size(); i++) {

				final StoreColumn<UUID> col = row.getByIndex(i);

				final boolean valid = col.getLong() > System.currentTimeMillis();
				final boolean match = col.getName().equals(id);

				LOG.trace(i + ": " + col.getName() + (!valid ? " (exp)" : ""));

				if (!valid) {

					// Expired column
					expired.add(col.getName());

					if (match) {
						throw new LockExpiredException();
					}

					continue;

				} else if (match) {

					// Lock acquired, overwrite with real expiration time
					register(TimeUnit.MILLISECONDS.convert(expiration, units));
					LOG.trace("locked: " + id);
					return true;

				} else {

					// First entry was unexpired and not a match, no lock acquired
					return false;

				}

			}

			return false;

		} catch (final LockExpiredException le) {

			throw le;

		} catch (final Exception e) {

			LOG.error("Error attempting to acquire lock " + name + ", retrying", e);

		} finally {

			if (expired.size() > 0) {
				cleanup(expired.toArray(new UUID[0]));
			}

		}

		return false;

	}

	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}

}