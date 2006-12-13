package com.kni.etl.util.aggregator;

public class Avg extends Aggregator {

    double bd;
    double counter = 0;
    int type;

    private static final Class[] validClasses = { int.class, float.class, double.class, short.class, long.class,
            byte.class, Integer.class, Float.class, Double.class, Short.class, Long.class, Byte.class };
    private static final int[] validClassTypes = { 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5 };


    
    
    @Override
    public void setValueClass(Class pOutputClass) throws AggregateException {
        super.setValueClass(pOutputClass);
        int res = com.kni.util.Arrays.searchArray(validClasses, pOutputClass);
        if (res < 0)
            throw new AggregateException("Cannot perform average on class type: " + pOutputClass.getName());

        type = validClassTypes[res];
    }
    

    @Override
    public void add(Object arg0) {
        if (counter == 0) {
            bd = ((Number) arg0).doubleValue();
        }
        else
            bd += ((Number) arg0).doubleValue();
        counter++;

    }

    @Override
    public Object getValue() {

        Double res = bd / counter;
        counter = 0;

        switch (type) {
        case 0:
            return res.intValue();
        case 1:
            return res.floatValue();
        case 2:
            return res.doubleValue();
        case 3:
            return res.shortValue();
        case 4:
            return res.longValue();
        case 5:
            return res.byteValue();
        default:
            return res.doubleValue();
        }

    }

    @Override
    public Class getValueClass() {
        return this.mOutputClass;
    }

}
