package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLTransformException;

public interface TransformBatchManager extends BatchManager {

    abstract Object[][] finishBatch(Object[][] data, int len) throws KETLTransformException;

    abstract Object[][] initializeBatch(Object[][] data, int len) throws KETLTransformException;
}
