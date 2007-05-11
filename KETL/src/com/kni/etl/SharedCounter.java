package com.kni.etl;

public class SharedCounter {

    int counter = 0;

    public synchronized int increment(int i) {
        this.counter += i;
        return this.counter;
    }

    public int value() {
        return this.counter;
    }

    public void set(int newValue) {
        this.counter = newValue;
    }

}
