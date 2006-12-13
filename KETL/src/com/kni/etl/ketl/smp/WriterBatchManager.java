package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLWriteException;

public interface WriterBatchManager extends BatchManager {

    public static final int NOCHANGE = 0;

    abstract int finishBatch(int len) throws KETLWriteException;

    abstract Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException;
}
