package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLWriteException;

public interface DefaultWriterCore extends DefaultCore {

    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException;
}
