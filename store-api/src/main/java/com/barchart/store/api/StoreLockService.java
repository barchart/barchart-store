package com.barchart.store.api;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public interface StoreLockService {

	/**
	 * Acquire a distributed lock with the specified name within the local datacenter. This provides the lowest latency
	 * if you do not need cross-datacenter locking.
	 *
	 * Note that you *cannot depend* on holding the same named lock simultaneously at multiple datacenters - it just
	 * doesn't check to see if the lock is currently held outside of the current datacenter in order to reduce latency
	 * for situations where you know lock requests will only come from one datacenter. In most situations, a local lock
	 * will behave exactly like a global lock.
	 *
	 * All locks must provide a failsafe expiration time to prevent stale locks from blocking other clients
	 * indefinitely. This expiration time only applies after the lock is already acquired; to set an expiration time for
	 * the acquisition itself, see {@link Lock#tryLock(long, TimeUnit)}.
	 */
	Lock localLock(String name, long expiration, TimeUnit units);

	/**
	 * Acquire a local lock with an expiration time of 60 seconds after acquisition.
	 */
	Lock localLock(String name);

	/**
	 * Acquire a distributed lock with the specified name across all datacenters. Acquiring a global lock can result in
	 * substantial latency and blocking behavior if you have multiple datacenters in your cluster.
	 *
	 * All locks must provide a failsafe expiration time to prevent stale locks from blocking other clients
	 * indefinitely. This expiration time only applies after the lock is already acquired; to set an expiration time for
	 * the acquisition itself, see {@link Lock#tryLock(long, TimeUnit)}.
	 */
	Lock globalLock(String name, long expiration, TimeUnit units);

	/**
	 * Acquire a global lock with an expiration time of 60 seconds after acquisition.
	 */
	Lock globalLock(String name);

}
