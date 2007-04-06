/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;


/**
 * Insert the type's description here.
 * Creation date: (5/9/2002 11:12:28 PM)
 * @author: Administrator
 */
public class PageParserPageParameter
{
    /**
    *
    */
    private static final long serialVersionUID = 3545512911192471347L;
    private char[] ParameterName;
    private char[] ParameterValue;
    public boolean ParameterRequired;
    public boolean RemoveParameterValue;
    public boolean RemoveParameter;
    public char[] ValueSeperator;

    /**
     * PageParserPageParameter constructor comment.
     */
    public PageParserPageParameter()
    {
        super();
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:50:24 AM)
     * @return java.lang.String
     */
    public char[] getParameterName()
    {
        return this.ParameterName;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:50:44 AM)
     * @return java.lang.String
     */
    public char[] getParameterValue()
    {
        return this.ParameterValue;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/12/2002 12:49:18 PM)
     * @return java.lang.String
     */
    public char[] getValueSeperator()
    {
        return this.ValueSeperator;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:51:10 AM)
     * @return boolean
     */
    public boolean isParameterRequired()
    {
        return this.ParameterRequired;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:51:54 AM)
     * @return boolean
     */
    public boolean isRemoveParameter()
    {
        return this.RemoveParameter;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:51:34 AM)
     * @return boolean
     */
    public boolean isRemoveParameterValue()
    {
        return this.RemoveParameterValue;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:50:24 AM)
     * @param newParameterName java.lang.String
     */
    public void setParameterName(java.lang.String newParameterName)
    {
        if (newParameterName == null)
        {
            return;
        }

        this.ParameterName = newParameterName.toCharArray();
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:51:10 AM)
     * @param newParameterRequired boolean
     */
    public void setParameterRequired(boolean newParameterRequired)
    {
        this.ParameterRequired = newParameterRequired;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:50:44 AM)
     * @param newParameterValue java.lang.String
     */
    public void setParameterValue(java.lang.String newParameterValue)
    {
        if (newParameterValue == null)
        {
            return;
        }

        this.ParameterValue = newParameterValue.toCharArray();
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:51:54 AM)
     * @param newRemoveParameter boolean
     */
    public void setRemoveParameter(boolean newRemoveParameter)
    {
        this.RemoveParameter = newRemoveParameter;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:51:34 AM)
     * @param newRemoveParameterValue boolean
     */
    public void setRemoveParameterValue(boolean newRemoveParameterValue)
    {
        this.RemoveParameterValue = newRemoveParameterValue;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/12/2002 12:49:18 PM)
     * @param newValueSeperator java.lang.String
     */
    public void setValueSeperator(java.lang.String newValueSeperator)
    {
        if (newValueSeperator == null)
        {
            return;
        }

        this.ValueSeperator = newValueSeperator.toCharArray();
    }
}
