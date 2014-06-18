package com.kni.etl.ketl;

import com.kni.etl.ketl.exceptions.KETLQAException;



public interface RecordChecker {

  /**
   * Record check.
   * 
   * @param di the di
   * @param e the e
   * 
   * @throws KETLQAException the KETLQA exception
   */
  public abstract void recordCheck(Object[] di, Exception e) throws KETLQAException;

}
