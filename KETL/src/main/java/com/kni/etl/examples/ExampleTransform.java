package com.kni.etl.examples;



import java.util.List;

import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.transformation.UserDefinedTransform;

public class ExampleTransform extends UserDefinedTransform {

  public ExampleTransform() {
    super();
  }

  @Override
  public void instantiate(List<Input> inputs) throws KETLTransformException {
    for (Input input : inputs)
      this.addOut(input.name(), input.inputClass());
  }


  @Override
  public void transform() throws InterruptedException, KETLTransformException, KETLQAException {
    for (Object[] r : this) {
      emit(r);
    }
  }

}
