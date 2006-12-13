package com.kni.etl.ketl.exceptions;


public class KETLError extends Error {

    public KETLError() {
        super();
    }

    public KETLError(String message, Throwable cause) {
        this(message);
        this.setStackTrace(cause.getStackTrace());
    }

    public KETLError(String message) {
        super(message);
    }

    public KETLError(Throwable cause) {
        super(cause.getMessage(),cause);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

}
