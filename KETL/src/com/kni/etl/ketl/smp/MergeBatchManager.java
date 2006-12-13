package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLTransformException;

public interface MergeBatchManager extends BatchManager {

    abstract Object[][] finishBatch(Object[][] data, int len) throws KETLTransformException;
}
