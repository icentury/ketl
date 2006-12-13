package com.kni.etl.util.aggregator;

public abstract class Aggregator {

    Class mOutputClass;


    
    public void setValueClass(Class cl) throws AggregateException {
        this.mOutputClass = cl;
    }

    abstract public Class getValueClass();

    abstract public void add(Object arg0);

    abstract public Object getValue();

}
