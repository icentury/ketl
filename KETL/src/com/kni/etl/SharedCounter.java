package com.kni.etl;



public class SharedCounter {

    int counter = 0;
    public synchronized int increment(int  i) {
        counter += i;
        return counter;
    }
    
    public int value() {
        return counter;
    }

    public void set(int newValue) {
        counter = newValue;
    }   

}
