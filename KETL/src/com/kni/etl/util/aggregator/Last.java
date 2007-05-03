package com.kni.etl.util.aggregator;

public class Last extends Aggregator {

    Object o;

    @Override
    public void add(Object arg0) {
        this.o = arg0;

    }

    @Override
    public Object getValue() {
        Object res = this.o;
        this.o = null;
        return res;
    }

    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
