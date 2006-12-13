package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLTransformException;

public interface SplitBatchManager extends BatchManager {

    abstract Object[][] initializeBatch(Object[][] data, int len) throws KETLTransformException;
}
