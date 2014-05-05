package com.kni.etl.ketl.smp;

public interface ETLStats {

	/**
	 * Update thread stats.
	 * 
	 * @param rowCount
	 *            the row count
	 */
	public abstract void updateThreadStats(int rowCount);

	public abstract void incrementTiming(long l);

	public abstract void setWaiting(Object object);

}