package com.kni.etl.ketl.lookup;

import java.io.Serializable;
import java.util.Date;

final public class SCDValue implements Serializable {

    private static final long serialVersionUID = 1L;
    private java.util.Date[] effDt;
    private java.util.Date[] expDt;
    private Integer[] keys;

    public SCDValue(Date[] effDt,Date[] expDt, Integer[] keys) {
        super();
        this.effDt = effDt;
        this.expDt = expDt;
        this.keys = keys;
    }

    final static public Object getNearestSCDValue(SCDValue data, java.util.Date eff) {

        // first part of array is dae second part is value
        if (data == null)
            return null;

        for (int i = data.effDt.length-1; i >= 0; i--) {
            if (eff.compareTo(data.effDt[i])>=0 && eff.compareTo(data.expDt[i]) <= 0)
                return data.keys[i];
        }

        return null;

    }
}
