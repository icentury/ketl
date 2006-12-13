/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.text.ParsePosition;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.stringtools.FastSimpleDateFormat;

/**
 * Insert the type's description here. Creation date: (3/26/2002 1:29:29 PM)
 *
 * @author: Administrator
 */
public class SourceFieldDefinition
{
    public int MaxLength;
    public int FixedLength = 0;
    public int AverageLength = 0;
    private java.lang.String Delimiter;
    int DelimeterLength;
    public char[] escapeCharacter = null;
    public Character escapeChar = null;
    public Class DataType;
    public boolean PartitionField = false;
    public boolean mEscapeDoubleQuotes = false;    
    public java.lang.String FormatString;
    public FastSimpleDateFormat DateFormatter = null;
    public ParsePosition position = null;
    public int ReadOrder;
    public int ReadOrderSequence;
    public boolean AutoTruncate = false;
    public java.lang.String DefaultValue;
    public String NullIf = null;
    public char[] NullIfCharArray = null;
    public boolean TrimValue = false;
    public int ObjectType = -1;
    private byte[] bDelimiter = null;
    private char[] cDelimiter = null;

    // any quote flag
    public boolean hasQuotes = false;

    // start quote
    private char[] cQuoteEnd = null;
    private byte[] bQuoteEnd = null;
    private String quoteEnd = null;
    private int quoteEndLength;

    // end quote
    private char[] cQuoteStart = null;
    private byte[] bQuoteStart = null;
    private String quoteStart = null;
    private int quoteStartLength;
    public boolean keepDelimiter;

    /**
     * SourceFieldDefinition constructor comment.
     */
    public SourceFieldDefinition()
    {
        super();

        /* field ends at delimiter */
        MaxLength = -1;

        /*
         * 1 = String, 2 = int, 3 = date, 4 = double, 5 = char, 6 = char array
         * of maxlength
         */
        DataType = null;

        /* 0 = Desc, 1 = Asc */
        ReadOrder = 0;

        /* Default to do not read data order by this column */
        ReadOrderSequence = -1;
    }

    public final byte[] getDelimiterAsBytes()
    {
        return this.bDelimiter;
    }

    public final char[] getDelimiterAsChars()
    {
        return this.cDelimiter;
    }

    public final byte[] getQuoteEndAsBytes()
    {
        return this.bQuoteEnd;
    }

    public final char[] getQuoteEndAsChars()
    {
        return this.cQuoteEnd;
    }

    public final void setQuoteEnd(java.lang.String pQuoteEnd)
    {
        quoteEnd = pQuoteEnd;

        if (quoteEnd != null)
        {
            hasQuotes = true;
            quoteEndLength = quoteEnd.length();
            this.bQuoteEnd = this.getQuoteEnd().getBytes();
            this.cQuoteEnd = this.getQuoteEnd().toCharArray();
        }
        else
        {
            quoteEndLength = 0;
            this.bQuoteEnd = null;
            this.cQuoteEnd = null;
        }
    }

    public final java.lang.String getQuoteEnd()
    {
        return quoteEnd;
    }

    public final void setEscapeDoubleQuotes(boolean arg0) {
        this.mEscapeDoubleQuotes = arg0;
    }
    
    public final boolean escapeDoubleQuotes() {
        return this.mEscapeDoubleQuotes;
    }
    
    public final void setEscapeCharacter(String pEscChar)
    {
        if (pEscChar == null || pEscChar.length()==0)
        {
            return;
        }

        this.escapeCharacter = pEscChar.toCharArray();
        this.escapeChar = escapeCharacter[0];

        if ((this.escapeCharacter != null) && (this.escapeCharacter.length > 1))
        {
            ResourcePool.LogMessage(this,ResourcePool.WARNING_MESSAGE,
                "Escape character can only be a single character not " + pEscChar + ", default to null");
        }
    }

    public final int getQuoteEndLength()
    {
        return quoteEndLength;
    }

    public final void setDelimiter(java.lang.String delimiter)
    {
        Delimiter = delimiter;

        if (delimiter != null)
        {
            DelimeterLength = delimiter.length();
            this.bDelimiter = this.getDelimiter().getBytes();
            this.cDelimiter = this.getDelimiter().toCharArray();
        }
        else
        {
            DelimeterLength = 0;
            this.bDelimiter = null;
            this.cDelimiter = null;
        }
    }

    public final java.lang.String getDelimiter()
    {
        return Delimiter;
    }

    public final int getDelimiterLength()
    {
        return DelimeterLength;
    }

    public final byte[] getQuoteStartAsBytes()
    {
        return this.bQuoteStart;
    }

    public final char[] getQuoteStartAsChars()
    {
        return this.cQuoteStart;
    }

    public final void setQuoteStart(java.lang.String pQuoteStart)
    {
        quoteStart = pQuoteStart;

        if (quoteStart != null)
        {
            hasQuotes = true;
            quoteStartLength = quoteStart.length();
            this.bQuoteStart = this.getQuoteStart().getBytes();
            this.cQuoteStart = this.getQuoteStart().toCharArray();
        }
        else
        {
            quoteStartLength = 0;
            this.bQuoteStart = null;
            this.cQuoteStart = null;
        }
    }

    public final java.lang.String getQuoteStart()
    {
        return quoteStart;
    }

    public final int getQuoteStartLength()
    {
        return quoteStartLength;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return "data type = " + DataType.getCanonicalName() + ", max length = " + MaxLength +
        ", delimeter = " + Delimiter + ", fixed width = " + FixedLength + ", format string = " + FormatString +
        ", read order = " + ReadOrder + ", read sequence = " + ReadOrderSequence + ", default value = " + DefaultValue +
        ", null if = " + NullIf + ", trim = " + TrimValue + ", object type = " +
        EngineConstants.resolveObjectIDToName(ObjectType);
    }

    /**
     * @param string
     */
    public void setNullIf(String string)
    {
        this.NullIf = string;   
        if(string != null)
            this.NullIfCharArray = string.toCharArray();
    }
}
