/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import com.kni.etl.stringtools.BoyerMooreAlgorithm;


/**
 * Insert the type's description here.
 * Creation date: (4/8/2002 3:34:16 PM)
 * @author: Administrator
 */
public class SessionIdentifier
{
    public int Weight;
    public int DataType;
    public int ObjectType;
    public BoyerMooreAlgorithm searchAccelerator = null;

    // 9 = MAIN_SESSION_IDENTIFIER, 10 = FIRST_CLICK_IDENTIFIER, 11 = BROWSER, 12 = IP_ADDRESS
    public int DestinationObjectType;
    public java.lang.String VariableName;
    public java.lang.String FormatString;
    public boolean CaseSensitive = false;

    /**
     * SessionIdentifier constructor comment.
     */
    public SessionIdentifier()
    {
        super();
    }

    /**
     * @param string
     */
    public void setVariableName(java.lang.String string, boolean caseSensitive)
    {
        if (string == null)
        {
            return;
        }

        CaseSensitive = caseSensitive;

        VariableName = string;

        if (VariableName != null)
        {
            searchAccelerator = new BoyerMooreAlgorithm();
            searchAccelerator.compile(VariableName);
        }
    }
}
