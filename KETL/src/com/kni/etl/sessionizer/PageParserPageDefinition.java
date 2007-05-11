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

import java.util.ArrayList;
import java.util.StringTokenizer;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/9/2002 11:08:57 PM)
 * 
 * @author: Administrator
 */
public class PageParserPageDefinition {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3257006544788009785L;
    
    /** The Host name. */
    private java.lang.String HostName;
    
    /** The Directory. */
    private java.lang.String Directory;
    
    /** The Protocol. */
    private java.lang.String Protocol;
    
    /** The Template. */
    private java.lang.String Template;
    
    /** The ID. */
    private int ID;
    
    /** The Host name as chars. */
    private char[] HostNameAsChars;
    
    /** The Directory as chars. */
    private char[] DirectoryAsChars;
    
    /** The Protocol as chars. */
    private char[] ProtocolAsChars;
    
    /** The Method as chars. */
    private char[] MethodAsChars;
    
    /** The Template as chars. */
    private char[] TemplateAsChars;
    
    /** The Valid status. */
    private int[] ValidStatus;
    
    /** The Valid page parameters. */
    private com.kni.etl.sessionizer.PageParserPageParameter[] ValidPageParameters;
    
    /** The Weight. */
    private int Weight;
    
    /** The valid. */
    private boolean valid = true;

    /**
     * PageParserPageDefinition constructor comment.
     */
    public PageParserPageDefinition() {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:09:49 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getDirectory() {
        return this.Directory;
    }

    /**
     * Sets the valid page.
     * 
     * @param pValid the new valid page
     */
    public void setValidPage(boolean pValid) {
        this.valid = pValid;
    }

    /**
     * Gets the valid page.
     * 
     * @return the valid page
     */
    public boolean getValidPage() {
        return this.valid;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:09:49 PM)
     * 
     * @return java.lang.String
     */
    public char[] getDirectoryAsCharArray() {
        return this.DirectoryAsChars;
    }

    /**
     * Gets the method as char array.
     * 
     * @return the method as char array
     */
    public char[] getMethodAsCharArray() {
        return this.MethodAsChars;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:09:25 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getHostName() {
        return this.HostName;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:09:25 PM)
     * 
     * @return java.lang.String
     */
    public char[] getHostNameAsCharArray() {
        return this.HostNameAsChars;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:10:09 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getProtocol() {
        return this.Protocol;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:10:09 PM)
     * 
     * @return java.lang.String
     */
    public char[] getProtocolAsCharArray() {
        return this.ProtocolAsChars;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:10:23 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getTemplate() {
        return this.Template;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:10:23 PM)
     * 
     * @return java.lang.String
     */
    public char[] getTemplateAsCharArray() {
        return this.TemplateAsChars;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:13:03 PM)
     * 
     * @return com.kni.etl.PageParserPageParameter[]
     */
    public com.kni.etl.sessionizer.PageParserPageParameter[] getValidPageParameters() {
        return this.ValidPageParameters;
    }

    /**
     * Insert the method's description here. Creation date: (5/10/2002 8:48:04 AM)
     * 
     * @return int
     */
    public int getWeight() {
        return this.Weight;
    }

    /**
     * Gets the valid status.
     * 
     * @return the valid status
     */
    public int[] getValidStatus() {
        return this.ValidStatus;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:09:49 PM)
     * 
     * @param newDirectory java.lang.String
     */
    public void setDirectory(java.lang.String newDirectory) {
        this.Directory = newDirectory;

        if (newDirectory != null) {
            this.DirectoryAsChars = newDirectory.toCharArray();
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:09:25 PM)
     * 
     * @param newHostName java.lang.String
     */
    public void setHostName(java.lang.String newHostName) {
        this.HostName = newHostName;

        if (newHostName != null) {
            this.HostNameAsChars = newHostName.toCharArray();
        }
    }

    /**
     * Sets the method.
     * 
     * @param newMethod the new method
     */
    public void setMethod(java.lang.String newMethod) {
        if (newMethod != null) {
            this.MethodAsChars = newMethod.toCharArray();
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:10:09 PM)
     * 
     * @param newProtocol java.lang.String
     */
    public void setProtocol(java.lang.String newProtocol) {
        this.Protocol = newProtocol;

        if (newProtocol != null) {
            this.ProtocolAsChars = newProtocol.toCharArray();
        }
    }

    /**
     * Sets the ID.
     * 
     * @param newID the new ID
     */
    public void setID(int newID) {
        this.ID = newID;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:10:23 PM)
     * 
     * @param newTemplate java.lang.String
     */
    public void setTemplate(String newTemplate) {
        this.Template = newTemplate;

        if ((newTemplate != null) && (newTemplate.length() > 0)) {
            this.TemplateAsChars = newTemplate.toCharArray();
        }
    }

    /**
     * Sets the valid status.
     * 
     * @param arg0 the new valid status
     */
    public void setValidStatus(String arg0) {
        // default to nothing
        this.ValidStatus = new int[0];

        if (arg0 != null) {
            StringTokenizer st = new StringTokenizer(arg0, ",");

            ArrayList a = new ArrayList();

            while (st.hasMoreTokens()) {
                a.add(st.nextToken());
            }

            this.ValidStatus = new int[a.size()];

            for (int i = 0; i < this.ValidStatus.length; i++) {
                this.ValidStatus[i] = Integer.parseInt((String) a.get(i));
            }
        }
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 11:13:03 PM)
     * 
     * @param newValidPageParameters com.kni.etl.PageParserPageParameter[]
     */
    public void setValidPageParameters(com.kni.etl.sessionizer.PageParserPageParameter[] newValidPageParameters) {
        this.ValidPageParameters = newValidPageParameters;
    }

    /**
     * Insert the method's description here. Creation date: (5/10/2002 8:48:04 AM)
     * 
     * @param newWeight int
     */
    public void setWeight(int newWeight) {
        this.Weight = newWeight;
    }

    /**
     * Gets the ID.
     * 
     * @return the ID
     */
    public int getID() {
        return this.ID;
    }
}
