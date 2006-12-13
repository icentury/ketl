package com.kni.etl.util.aggregator;

final public class Count extends Aggregator {

    private int counter = 0;


    @Override
    public void add(Object arg0) {
        counter++;
    }

    @Override
    public Object getValue() {
        Integer res = counter;
        counter = 0;
        return res;
    }

    @Override
    public Class getValueClass() {
        return Integer.class;
    }

}
