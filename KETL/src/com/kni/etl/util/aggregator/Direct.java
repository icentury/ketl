package com.kni.etl.util.aggregator;

public class Direct extends Aggregator {

    Object o;

    @Override
    public void add(Object arg0) {
        this.o = arg0;

    }

    @Override
    public Object getValue() {
        return this.o;
    }

    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
