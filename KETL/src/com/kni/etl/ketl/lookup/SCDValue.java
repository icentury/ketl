/*
 *  Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.lookup;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

// TODO: Auto-generated Javadoc
/**
 * The Class SCDValue.
 */
final public class SCDValue implements Externalizable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The eff dt. */
    private java.util.Date[] effDt;
    
    /** The exp dt. */
    private java.util.Date[] expDt;
    
    /** The keys. */
    private Integer[] keys;

    /**
     * Instantiates a new SCD value.
     */
    public SCDValue() {
        super();
    }
    
    /**
     * Instantiates a new SCD value.
     * 
     * @param effDt the eff dt
     * @param expDt the exp dt
     * @param keys the keys
     */
    public SCDValue(Date[] effDt, Date[] expDt, Integer[] keys) {
        super();
        this.effDt = effDt;
        this.expDt = expDt;
        this.keys = keys;
    }

    /**
     * Gets the nearest SCD value.
     * 
     * @param data the data
     * @param eff the eff
     * 
     * @return the nearest SCD value
     */
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

    /* (non-Javadoc)
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readShort();
        this.effDt = new java.util.Date[size];
        this.expDt = new java.util.Date[size];
        this.keys = new Integer[size];
        for (int i = 0; i < size; i++) {
            this.keys[i] = in.readInt();
            this.effDt[i] = new java.util.Date(in.readLong());
            this.expDt[i] = new java.util.Date(in.readLong());
        }
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
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
