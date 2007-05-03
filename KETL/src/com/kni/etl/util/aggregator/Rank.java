package com.kni.etl.util.aggregator;

public class Rank extends Aggregator {

    int Counter = 1;

    @Override
    public void add(Object arg0) {
    }

    @Override
    public Object getValue() {
        return this.Counter++;
    }

    @Override
    public Class getValueClass() {
        return Integer.class;
    }

}
