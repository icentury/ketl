package com.kni.etl.ketl.lookup;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

final public class SCDValue implements Externalizable {

    private static final long serialVersionUID = 1L;
    private java.util.Date[] effDt;
    private java.util.Date[] expDt;
    private Integer[] keys;

    public SCDValue() {
        super();
    }
    
    public SCDValue(Date[] effDt, Date[] expDt, Integer[] keys) {
        super();
        this.effDt = effDt;
        this.expDt = expDt;
        this.keys = keys;
    }

    final static public Object getNearestSCDValue(SCDValue data, java.util.Date eff) {

        // first part of array is dae second part is value
        if (data == null)
            return null;

        for (int i = data.effDt.length - 1; i >= 0; i--) {
            if (eff.compareTo(data.effDt[i]) >= 0 && eff.compareTo(data.expDt[i]) <= 0)
                return data.keys[i];
        }

        return null;

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = (int) in.readShort();
        effDt = new java.util.Date[size];
        expDt = new java.util.Date[size];
        keys = new Integer[size];
        for (int i = 0; i < size; i++) {
            keys[i] = in.readInt();
            effDt[i] = new java.util.Date(in.readLong());
            expDt[i] = new java.util.Date(in.readLong());
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        int len = this.keys.length;
        out.writeShort(len);
        for (int i = 0; i < len; i++) {
            out.writeInt(this.keys[i].intValue());
            out.writeLong(this.effDt[i].getTime());
            out.writeLong(this.expDt[i].getTime());
        }

    }
}
