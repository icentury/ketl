package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.transformation.UserDefinedTransform;

public interface JavaTransformCore extends DefaultCore {
  public UserDefinedTransform getTransform();
}
