/**
 * 
 */
package com.kni.etl.ketl.writer;

import com.kni.etl.ketl.exceptions.KETLWriteException;

public class ForcedException extends KETLWriteException {

    public ForcedException(String message) {
        super(message);
    }

    private static final long serialVersionUID = -2398460000118764505L;

}