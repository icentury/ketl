/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl_v1;

import java.io.IOException;
import java.io.Serializable;



/**
 * Insert the type's description here.
 * Creation date: (3/26/2002 5:36:51 PM)
 * @author: Administrator
 */
public class ResultRecord implements Cloneable, Serializable
{
    /**
     *
     */
    private static final long serialVersionUID = 3257281439925351992L;
    public int Type = 0;
    public DataItem[] LineFields;
    public String SourceFile = null;
    public double SourceLine = 0;
    public double OverallLine = 0;
    public long bytes = 0;

    /**
     * ResultRecord constructor comment.
     */
    public ResultRecord()
    {
        super();
    }

    /**
     * ResultRecord constructor comment.
     */
    public ResultRecord(int numberOfDataItems)
    {
        this();

        LineFields = new DataItem[numberOfDataItems];

        for (int i = 0; i < numberOfDataItems; i++)
        {
            LineFields[i] = new DataItem();
        }
    }

    public ResultRecord(ResultRecord rr)
    {
        this();

        if (rr.LineFields != null)
        {
            // duplicate list items
            int size = rr.LineFields.length;

            LineFields = new DataItem[size];

            for (int i = 0; i < size; i++)
            {
                LineFields[i] = new DataItem(rr.LineFields[i]);
            }
        }

        SourceFile = rr.SourceFile;
        OverallLine = rr.OverallLine;
        SourceLine = rr.SourceLine;
        Type = rr.Type;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 11:41:24 PM)
     */
    public Object clone() throws CloneNotSupportedException
    {
        ResultRecord newRecord = null;

        newRecord = (ResultRecord) super.clone();

        if (LineFields != null)
        {
            // duplicate list items
            int size = LineFields.length;

            newRecord.LineFields = new DataItem[size];

            for (int i = 0; i < size; i++)
            {
                newRecord.LineFields[i] = new DataItem(LineFields[i]);
            }
        }

        newRecord.SourceFile = this.SourceFile;
        newRecord.OverallLine = this.OverallLine;
        newRecord.SourceLine = this.SourceLine;
        newRecord.Type = this.Type;

        return newRecord;
    }

    public ResultRecord CopyTo(ResultRecord newRecord)
    {
        // duplicate list items
        int size = LineFields.length;

        for (int i = 0; i < size; i++)
        {
            newRecord.LineFields[i].set(LineFields[i]);
        }

        newRecord.SourceFile = this.SourceFile;
        newRecord.OverallLine = this.OverallLine;
        newRecord.SourceLine = this.SourceLine;
        newRecord.Type = this.Type;

        return newRecord;
    }

    /**
     * Check result record for any errors and return index of field with error
     * @param ResultRecord to check
     * @return Field with error
     */
    public static int hasErrors(ResultRecord r)
    {
        int len = r.LineFields.length;

        for (int i = 0; i < len; i++)
        {
            if ((r.LineFields[i] == null) || (r.LineFields[i].valError != null))
            {
                return i;
            }
        }

        return -1;
    }

    /**
     * Insert the method's description here.
     * Creation date: (4/20/2002 7:47:49 PM)
     */
    public void sync()
    {
    }

    public String toString()
    {
        if (LineFields == null)
        {
            return null;
        }

        if (LineFields.length == 0)
        {
            return "(no items)";
        }

        String s = LineFields[0].toString();

        for (int i = 1; i < LineFields.length; i++)
        {
            s = s + " | " + LineFields[i];
        }

        return s; //LineFields.toString();
    }

    public void readExternal(java.io.ObjectInput s) throws ClassNotFoundException, IOException
    {
        Type = s.readInt();
        SourceFile = s.readUTF();
        LineFields = (DataItem[]) s.readObject();
        SourceLine = s.readDouble();
        OverallLine = s.readDouble();
        bytes = s.readLong();
    }

    public void writeExternal(java.io.ObjectOutput s) throws IOException
    {
        s.writeInt(Type);
        s.writeUTF(SourceFile);
        s.writeObject(LineFields);
        s.writeDouble(SourceLine);
        s.writeDouble(OverallLine);
        s.writeLong(bytes);
    }
}
