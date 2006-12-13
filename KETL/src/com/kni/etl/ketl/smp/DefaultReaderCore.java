package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLReadException;

public interface DefaultReaderCore extends DefaultCore {

    public final static int SKIP_RECORD = 0;
    public final static int SUCCESS = 1;
    public final static int COMPLETE = 2;

    public abstract int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLReadException;
}
