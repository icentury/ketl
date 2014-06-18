package com.kni.etl.ketl.smp;



public interface AggregateTransformCore {

  /**
   * Aggregate batch.
   * 
   * @param res the res
   * @param length the length
   * 
   * @throws InterruptedException the interrupted exception
   */
  public abstract void aggregateBatch(Object[][] res, int length) throws InterruptedException;

}
