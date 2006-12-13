/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.util.ArrayList;
import java.util.StringTokenizer;


/**
 * Insert the type's description here.
 * Creation date: (5/9/2002 11:08:57 PM)
 * @author: Administrator
 */
public class PageParserPageDefinition
{
    /**
     *
     */
    private static final long serialVersionUID = 3257006544788009785L;
    private java.lang.String HostName;
    private java.lang.String Directory;
    private java.lang.String Method;
    private java.lang.String Protocol;
    private java.lang.String Template;
    private int ID;
    private char[] HostNameAsChars;
    private char[] DirectoryAsChars;
    private char[] ProtocolAsChars;
    private char[] MethodAsChars;
    private char[] TemplateAsChars;
    private int[] ValidStatus;
    private com.kni.etl.sessionizer.PageParserPageParameter[] ValidPageParameters;
    private int Weight;
    private boolean valid = true;

    /**
     * PageParserPageDefinition constructor comment.
     */
    public PageParserPageDefinition()
    {
        super();
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:09:49 PM)
     * @return java.lang.String
     */
    public java.lang.String getDirectory()
    {
        return Directory;
    }

    public void setValidPage(boolean pValid)
    {
        this.valid = pValid;
    }

    public boolean getValidPage()
    {
        return this.valid;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:09:49 PM)
     * @return java.lang.String
     */
    public char[] getDirectoryAsCharArray()
    {
        return this.DirectoryAsChars;
    }
    
    public char[] getMethodAsCharArray()
    {
        return this.MethodAsChars;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:09:25 PM)
     * @return java.lang.String
     */
    public java.lang.String getHostName()
    {
        return HostName;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:09:25 PM)
     * @return java.lang.String
     */
    public char[] getHostNameAsCharArray()
    {
        return this.HostNameAsChars;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:10:09 PM)
     * @return java.lang.String
     */
    public java.lang.String getProtocol()
    {
        return Protocol;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:10:09 PM)
     * @return java.lang.String
     */
    public char[] getProtocolAsCharArray()
    {
        return this.ProtocolAsChars;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:10:23 PM)
     * @return java.lang.String
     */
    public java.lang.String getTemplate()
    {
        return Template;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:10:23 PM)
     * @return java.lang.String
     */
    public char[] getTemplateAsCharArray()
    {
        return this.TemplateAsChars;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:13:03 PM)
     * @return com.kni.etl.PageParserPageParameter[]
     */
    public com.kni.etl.sessionizer.PageParserPageParameter[] getValidPageParameters()
    {
        return ValidPageParameters;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:48:04 AM)
     * @return int
     */
    public int getWeight()
    {
        return Weight;
    }

    public int[] getValidStatus()
    {
        return this.ValidStatus;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:09:49 PM)
     * @param newDirectory java.lang.String
     */
    public void setDirectory(java.lang.String newDirectory)
    {
        this.Directory = newDirectory;

        if (newDirectory != null)
        {
            this.DirectoryAsChars = newDirectory.toCharArray();
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:09:25 PM)
     * @param newHostName java.lang.String
     */
    public void setHostName(java.lang.String newHostName)
    {
        this.HostName = newHostName;

        if (newHostName != null)
        {
            this.HostNameAsChars = newHostName.toCharArray();
        }
    }
    
    
    public void setMethod(java.lang.String newMethod)
    {
        this.Method = newMethod;

        if (newMethod != null)
        {
            this.MethodAsChars = newMethod.toCharArray();
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:10:09 PM)
     * @param newProtocol java.lang.String
     */
    public void setProtocol(java.lang.String newProtocol)
    {
        this.Protocol = newProtocol;

        if (newProtocol != null)
        {
            this.ProtocolAsChars = newProtocol.toCharArray();
        }
    }

    public void setID(int newID)
    {
        this.ID = newID;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:10:23 PM)
     * @param newTemplate java.lang.String
     */
    public void setTemplate(String newTemplate)
    {
        this.Template = newTemplate;

        if ((newTemplate != null) && (newTemplate.length() > 0))
        {
            this.TemplateAsChars = newTemplate.toCharArray();
        }
    }

    public void setValidStatus(String arg0)
    {
        // default to nothing
        this.ValidStatus = new int[0];

        if (arg0 != null)
        {
            StringTokenizer st = new StringTokenizer(arg0, ",");

            ArrayList a = new ArrayList();

            while (st.hasMoreTokens())
            {
                a.add(st.nextToken());
            }

            this.ValidStatus = new int[a.size()];

            for (int i = 0; i < this.ValidStatus.length; i++)
            {
                this.ValidStatus[i] = Integer.parseInt((String) a.get(i));
            }
        }
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/9/2002 11:13:03 PM)
     * @param newValidPageParameters com.kni.etl.PageParserPageParameter[]
     */
    public void setValidPageParameters(com.kni.etl.sessionizer.PageParserPageParameter[] newValidPageParameters)
    {
        ValidPageParameters = newValidPageParameters;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 8:48:04 AM)
     * @param newWeight int
     */
    public void setWeight(int newWeight)
    {
        Weight = newWeight;
    }

    /**
     * @return
     */
    public int getID()
    {
        return ID;
    }
}
