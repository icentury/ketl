package com.kni.etl.ketl.smp;

public interface KETLExceptionWrapper<T extends Exception> {
  public void wrapException(Exception e) throws T;
}
