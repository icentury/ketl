package com.kni.etl.util.aggregator;

public class ToArray extends Aggregator {

    int arrayLimit = 1024 * 64;
    Object[] o = new Object[this.arrayLimit];
    int pos = 0;

    @Override
    public void add(Object arg0) {
        if (this.pos == this.arrayLimit)
            throw new AggregateException(
                    "Max elements for ToArray has been reached, increase MAXELEMENTS value, current limit = "
                            + this.arrayLimit);
        this.o[this.pos++] = arg0;
    }

    public void setArrayLimit(int limit) {
        if (limit < 1)
            throw new AggregateException("Array limit must be greater than 0");
        this.arrayLimit = limit;
        this.o = new Object[this.arrayLimit];
    }

    @Override
    public Object getValue() {
        Object[] res = (Object[]) java.lang.reflect.Array.newInstance(this.mOutputClass, this.pos);
        System.arraycopy(this.o, 0, res, 0, this.pos);
        this.pos = 0;
        return res;
    }

    @Override
    public Class getValueClass() {
        return java.lang.reflect.Array.newInstance(this.mOutputClass, 0).getClass();
    }

}
