/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;

/**
 * Insert the type's description here. Creation date: (3/26/2002 1:29:29 PM)
 * 
 * @author: Administrator
 */
public class DestinationFieldDefinition {

    public int MaxLength = -1;
    public boolean FixedWidth = false;
    public String Delimiter;
    public String FormatString = null;
    public String DefaultValue;
    public boolean AppendLineFeed = false;

    // cached members
    private SimpleDateFormat sSimpleDateFormatter = null;
    private DecimalFormat sDecimalFormatter = null;
    private byte[] bDelimiter = null;
    private byte[] bDefeaultValue = null;
    private String mCharSet = null;

    /**
     * SourceFieldDefinition constructor comment.
     * 
     * @param pCharSet TODO
     */
    public DestinationFieldDefinition(String pCharSet) {
        super();
        this.mCharSet = pCharSet;
    }

    public byte[] getDelimiterAsBytes() throws UnsupportedEncodingException {
        if ((this.bDelimiter == null) && (this.Delimiter != null)) {
            this.bDelimiter = this.mCharSet == null ? this.Delimiter.getBytes() : this.Delimiter
                    .getBytes(this.mCharSet);
        }

        return this.bDelimiter;
    }

    public byte[] getDefaultValueAsBytes() throws UnsupportedEncodingException {
        if ((this.bDefeaultValue == null) && (this.DefaultValue != null)) {
            this.bDefeaultValue = this.mCharSet == null ? this.DefaultValue.getBytes() : this.DefaultValue
                    .getBytes(this.mCharSet);
        }

        return this.bDefeaultValue;
    }

    public SimpleDateFormat getSimpleDateFormat() {
        if (this.sSimpleDateFormatter == null) {
            this.sSimpleDateFormatter = new SimpleDateFormat();
        }

        if (this.FormatString != null) {
            this.sSimpleDateFormatter.applyPattern(this.FormatString);
        }

        return (this.sSimpleDateFormatter);
    }

    public DecimalFormat getDecimalFormat() {
        if (this.sDecimalFormatter == null) {
            this.sDecimalFormatter = new DecimalFormat();
        }

        if (this.FormatString != null) {
            this.sDecimalFormatter.applyPattern(this.FormatString);
        }

        return (this.sDecimalFormatter);
    }

    public Format getFormat(Class cl) {
        if (Number.class.isAssignableFrom(cl))
            return this.getDecimalFormat();

        if (java.util.Date.class.isAssignableFrom(cl))
            return this.getSimpleDateFormat();

        return null;

    }
}
