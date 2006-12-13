package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLReadException;

public interface ReaderBatchManager extends BatchManager {

    public static final int NOCHANGE = 0;

    abstract Object[][] finishBatch(Object[][] data, int len) throws KETLReadException;

    abstract void initializeBatch() throws KETLReadException;
}
