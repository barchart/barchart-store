package com.barchart.store.util;

import java.util.UUID;

import com.eaio.uuid.UUIDGen;

public final class UUIDUtil {

	// This comes from Hector's TimeUUIDUtils class:
	// https://github.com/rantav/hector/blob/master/core/src/main/java/me/...
	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH =
			0x01b21dd213814000L;

	public static final UUID timeUUID() {
		return new UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode());
	}

	public static final UUID timeUUID(final long time) {
		return new UUID(UUIDGen.createTime(time), UUIDGen.getClockSeqAndNode());
	}

	public static final long timestampFrom(final UUID uuid) {
		return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

}
