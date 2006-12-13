package com.kni.etl.ketl.exceptions;


public class KETLException extends Exception {
    /**
    *
    */
    private static final long serialVersionUID = 4048797879017486642L;

    /**
         *
         */
    public KETLException()
    {
        super();
    }

    /**
     * @param message
     */
    public KETLException(String message)
    {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public KETLException(String message, Throwable cause)
    {
        super(message,cause);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * @param cause
     */
    public KETLException(Throwable cause)
    {
        super(cause.getMessage(),cause);
        this.setStackTrace(cause.getStackTrace());
    }
}
