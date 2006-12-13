package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLTransformException;

public interface DefaultTransformCore extends DefaultCore {

    public final static int SKIP_RECORD = 0;
    public final static int SUCCESS = 1;
    public final static int REPEAT_RECORD = 2;

    public abstract int transformRecord(Object[] pInputRecord, Class[] pInputDataTypes, int pInputRecordWidth,
            Object[] pOutputRecord, Class[] pOutputDataTypes, int pOutputRecordWidth) throws KETLTransformException;
}
