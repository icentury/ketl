package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLTransformException;

public interface DefaultSplitCore extends DefaultCore {

    public final static int SKIP_RECORD = 0;
    public final static int SUCCESS = 1;

    public abstract int splitRecord(Object[] pInputRecord, Class[] pInputDataTypes, int pInputRecordWidth,
            int pOutPath, Object[] pOutputRecord, Class[] pOutputDataTypes, int pOutputRecordWidth)
            throws KETLTransformException;
}
