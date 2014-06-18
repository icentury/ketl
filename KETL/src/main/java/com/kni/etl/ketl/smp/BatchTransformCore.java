package com.kni.etl.ketl.smp;

import java.util.concurrent.BlockingQueue;

import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLTransformException;


public interface BatchTransformCore extends DefaultCore {

  public abstract void iterableTransform(BlockingQueue<Object[][]> sourceQueue,
      BlockingQueue<Object[][]> queue);

  public abstract void transform() throws InterruptedException, KETLTransformException,
      KETLQAException;

  public abstract void close() throws InterruptedException, KETLTransformException;

}
