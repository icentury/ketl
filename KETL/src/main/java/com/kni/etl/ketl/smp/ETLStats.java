package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLException;


public interface ETLStats {

  /**
   * Update thread stats.
   * 
   * @param rowCount the row count
   */

  public abstract void incrementTiming(long l);

  public abstract void setWaiting(Object object);

  public abstract void updateThreadStats(int rowCount);

  public abstract void incrementErrorCount(Exception e, Object[] objects, int val)
      throws KETLException;

  public abstract void incrementErrorCount(Exception e, Object[] lObject, Object[] rObject, int val)
      throws KETLException;


}
