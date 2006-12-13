package com.kni.etl.util.aggregator;

public class First extends Aggregator {

    Object o = null;

 
    @Override
    public void add(Object arg0) {
        if (o == null)
            o = arg0;

    }

    @Override
    public Object getValue() {
        Object res = o;
        o = null;
        return res;
    }

    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
