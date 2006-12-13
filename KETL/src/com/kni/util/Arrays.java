package com.kni.util;


public class Arrays  {

    static public int searchArray(Object[] items, Object value) {
        for (int i = 0; i < items.length; i++) {
            if (items[i].equals(value))
                return i;
        }
    
        return -1;
    }

}
