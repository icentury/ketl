/*
 *  Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
package com.kni.etl.sessionizer;

import com.kni.etl.stringtools.BoyerMooreAlgorithm;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (4/8/2002 3:34:16 PM)
 * 
 * @author: Administrator
 */
public class SessionIdentifier {

    /** The Weight. */
    public int Weight;
    // public int DataType;
    /** The Object type. */
    public int ObjectType;
    
    /** The search accelerator. */
    public BoyerMooreAlgorithm searchAccelerator = null;
    
    /** The Destination object type. */
    public int DestinationObjectType;
    
    /** The Variable name. */
    public java.lang.String VariableName;
    
    /** The Format string. */
    public java.lang.String FormatString;
    
    /** The Case sensitive. */
    public boolean CaseSensitive = false;
    
    /** The identifiers. */
    public int identifiers = 0;
    
    /** The identifier. */
    public int[] identifier;

    /**
     * Adds the session identifier map.
     * 
     * @param pos the pos
     */
    public void addSessionIdentifierMap(int pos) {
        if (this.identifiers == 0)
            this.identifier = new int[this.identifiers + 1];
        else {
            int[] tmp = new int[this.identifiers + 1];
            System.arraycopy(this.identifier, 0, tmp, 0, this.identifiers);
            this.identifier = tmp;
        }
        this.identifier[this.identifiers++] = pos;
    }

    /**
     * SessionIdentifier constructor comment.
     */
    public SessionIdentifier() {
        super();
    }

    /**
     * Sets the variable name.
     * 
     * @param string the string
     * @param caseSensitive the case sensitive
     */
    public void setVariableName(java.lang.String string, boolean caseSensitive) {
        if (string == null) {
            return;
        }

        this.CaseSensitive = caseSensitive;

        this.VariableName = string;

        if (this.VariableName != null) {
            this.searchAccelerator = new BoyerMooreAlgorithm();
            this.searchAccelerator.compile(this.VariableName);
        }
    }
}
