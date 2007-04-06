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
    //public int DataType;
    public int ObjectType;
    public BoyerMooreAlgorithm searchAccelerator = null;
    public int DestinationObjectType;
    public java.lang.String VariableName;
    public java.lang.String FormatString;
    public boolean CaseSensitive = false;
    public int identifiers = 0;
    public int[] identifier;

    
    public void addSessionIdentifierMap(int pos){
        if(this.identifiers ==0)
            this.identifier = new int[this.identifiers+1];
        else {
            int[] tmp = new int[this.identifiers+1];
            System.arraycopy(this.identifier,0, tmp, 0,this.identifiers);
            this.identifier = tmp;
        }        
        this.identifier[this.identifiers++] = pos;        
    }
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

        this.CaseSensitive = caseSensitive;

        this.VariableName = string;

        if (this.VariableName != null)
        {
            this.searchAccelerator = new BoyerMooreAlgorithm();
            this.searchAccelerator.compile(this.VariableName);
        }
    }
}
