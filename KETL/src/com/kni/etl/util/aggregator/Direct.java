package com.kni.etl.util.aggregator;

public class Direct extends Aggregator {

    Object o;



    @Override
    public void add(Object arg0) {
        o = arg0;

    }

    @Override
    public Object getValue() {
        return o;
    }

    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
