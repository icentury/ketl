package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLTransformException;

public interface DefaultMergeCore extends DefaultCore {

    public final static int SUCCESS_ADVANCE_LEFT = 2;
    public final static int SUCCESS_ADVANCE_RIGHT = 3;
    public final static int SUCCESS_ADVANCE_BOTH = 4;
    public final static int SKIP_ADVANCE_LEFT = 5;
    public final static int SKIP_ADVANCE_RIGHT = 6;
    public final static int SKIP_ADVANCE_BOTH = 7;

    public int mergeRecord(Object[] pLeftInputRecord, Class[] pLeftInputDataTypes, int pLeftInputRecordWidth,
            Object[] pRightInputRecord, Class[] pRightInputDataTypes, int pRightInputRecordWidth,
            Object[] pOutputRecord, Class[] pOutputDataTypes, int pOutputRecordWidth) throws KETLTransformException;
}
