package com.kni.etl.ketl.transformation;

public interface SharedResource {
  void setSharedResource(String field, Object value);

  Object getSharedResource(String field);
}
