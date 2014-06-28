/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
 */
package com.kni.etl;

import java.text.ParsePosition;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.stringtools.FastSimpleDateFormat;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (3/26/2002 1:29:29 PM)
 * 
 * @author: Administrator
 */
public class SourceFieldDefinition {

  /** The Max length. */
  public int MaxLength;

  /** The Fixed length. */
  public int FixedLength = 0;

  /** The Average length. */
  public int AverageLength = 0;

  /** The Delimiter. */
  private java.lang.String Delimiter;

  /** The Delimeter length. */
  int DelimeterLength;

  /** The escape character. */
  public char[] escapeCharacter = null;

  /** The escape char. */
  public Character escapeChar = null;

  /** The Data type. */
  public Class DataType;

  /** The Partition field. */
  public boolean PartitionField = false;

  /** The escape double quotes. */
  public boolean mEscapeDoubleQuotes = false;

  /** The Format string. */
  public java.lang.String FormatString;

  /** The Date formatter. */
  public FastSimpleDateFormat DateFormatter = null;

  /** The position. */
  public ParsePosition position = null;

  /** The Read order. */
  public int ReadOrder;

  /** The Read order sequence. */
  public int ReadOrderSequence;

  /** The Auto truncate. */
  public boolean AutoTruncate = false;

  /** The Default value. */
  public java.lang.String DefaultValue;

  /** The Null if. */
  public String NullIf = null;

  /** The Null if char array. */
  public char[] NullIfCharArray = null;

  /** The Trim value. */
  public boolean TrimValue = false;

  /** The Object type. */
  public int ObjectType = -1;

  /** The b delimiter. */
  private byte[] bDelimiter = null;

  /** The c delimiter. */
  private char[] cDelimiter = null;

  // any quote flag
  /** The has quotes. */
  public boolean hasQuotes = false;

  // start quote
  /** The c quote end. */
  private char[] cQuoteEnd = null;

  /** The b quote end. */
  private byte[] bQuoteEnd = null;

  /** The quote end. */
  private String quoteEnd = null;

  /** The quote end length. */
  private int quoteEndLength;

  // end quote
  /** The c quote start. */
  private char[] cQuoteStart = null;

  /** The b quote start. */
  private byte[] bQuoteStart = null;

  /** The quote start. */
  private String quoteStart = null;

  /** The quote start length. */
  private int quoteStartLength;

  /** The keep delimiter. */
  public boolean keepDelimiter;

  public enum Internal {
    FILENAME, FILEPATH, FILE_ID
  };

  public Internal internal = null;

  /**
   * SourceFieldDefinition constructor comment.
   */
  public SourceFieldDefinition() {
    super();

    /* field ends at delimiter */
    this.MaxLength = -1;

    /*
     * 1 = String, 2 = int, 3 = date, 4 = double, 5 = char, 6 = char array of maxlength
     */
    this.DataType = null;

    /* 0 = Desc, 1 = Asc */
    this.ReadOrder = 0;

    /* Default to do not read data order by this column */
    this.ReadOrderSequence = -1;
  }

  /**
   * Gets the delimiter as bytes.
   * 
   * @return the delimiter as bytes
   */
  public final byte[] getDelimiterAsBytes() {
    return this.bDelimiter;
  }

  /**
   * Gets the delimiter as chars.
   * 
   * @return the delimiter as chars
   */
  public final char[] getDelimiterAsChars() {
    return this.cDelimiter;
  }

  /**
   * Gets the quote end as bytes.
   * 
   * @return the quote end as bytes
   */
  public final byte[] getQuoteEndAsBytes() {
    return this.bQuoteEnd;
  }

  /**
   * Gets the quote end as chars.
   * 
   * @return the quote end as chars
   */
  public final char[] getQuoteEndAsChars() {
    return this.cQuoteEnd;
  }

  /**
   * Sets the quote end.
   * 
   * @param pQuoteEnd the new quote end
   */
  public final void setQuoteEnd(java.lang.String pQuoteEnd) {
    this.quoteEnd = pQuoteEnd;

    if (this.quoteEnd != null) {
      this.hasQuotes = true;
      this.quoteEndLength = this.quoteEnd.length();
      this.bQuoteEnd = this.getQuoteEnd().getBytes();
      this.cQuoteEnd = this.getQuoteEnd().toCharArray();
    } else {
      this.quoteEndLength = 0;
      this.bQuoteEnd = null;
      this.cQuoteEnd = null;
    }
  }

  /**
   * Gets the quote end.
   * 
   * @return the quote end
   */
  public final java.lang.String getQuoteEnd() {
    return this.quoteEnd;
  }

  /**
   * Sets the escape double quotes.
   * 
   * @param arg0 the new escape double quotes
   */
  public final void setEscapeDoubleQuotes(boolean arg0) {
    this.mEscapeDoubleQuotes = arg0;
  }

  /**
   * Escape double quotes.
   * 
   * @return true, if successful
   */
  public final boolean escapeDoubleQuotes() {
    return this.mEscapeDoubleQuotes;
  }

  /**
   * Sets the escape character.
   * 
   * @param pEscChar the new escape character
   */
  public final void setEscapeCharacter(String pEscChar) {
    if (pEscChar == null || pEscChar.length() == 0) {
      return;
    }

    this.escapeCharacter = pEscChar.toCharArray();
    this.escapeChar = this.escapeCharacter[0];

    if ((this.escapeCharacter != null) && (this.escapeCharacter.length > 1)) {
      ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
          "Escape character can only be a single character not " + pEscChar + ", default to null");
    }
  }

  /**
   * Gets the quote end length.
   * 
   * @return the quote end length
   */
  public final int getQuoteEndLength() {
    return this.quoteEndLength;
  }

  /**
   * Sets the delimiter.
   * 
   * @param delimiter the new delimiter
   */
  public final void setDelimiter(java.lang.String delimiter) {
    this.Delimiter = delimiter;

    if (delimiter != null) {
      this.DelimeterLength = delimiter.length();
      this.bDelimiter = this.getDelimiter().getBytes();
      this.cDelimiter = this.getDelimiter().toCharArray();
    } else {
      this.DelimeterLength = 0;
      this.bDelimiter = null;
      this.cDelimiter = null;
    }
  }

  /**
   * Gets the delimiter.
   * 
   * @return the delimiter
   */
  public final java.lang.String getDelimiter() {
    return this.Delimiter;
  }

  /**
   * Gets the delimiter length.
   * 
   * @return the delimiter length
   */
  public final int getDelimiterLength() {
    return this.DelimeterLength;
  }

  /**
   * Gets the quote start as bytes.
   * 
   * @return the quote start as bytes
   */
  public final byte[] getQuoteStartAsBytes() {
    return this.bQuoteStart;
  }

  /**
   * Gets the quote start as chars.
   * 
   * @return the quote start as chars
   */
  public final char[] getQuoteStartAsChars() {
    return this.cQuoteStart;
  }

  /**
   * Sets the quote start.
   * 
   * @param pQuoteStart the new quote start
   */
  public final void setQuoteStart(java.lang.String pQuoteStart) {
    this.quoteStart = pQuoteStart;

    if (this.quoteStart != null) {
      this.hasQuotes = true;
      this.quoteStartLength = this.quoteStart.length();
      this.bQuoteStart = this.getQuoteStart().getBytes();
      this.cQuoteStart = this.getQuoteStart().toCharArray();
    } else {
      this.quoteStartLength = 0;
      this.bQuoteStart = null;
      this.cQuoteStart = null;
    }
  }

  /**
   * Gets the quote start.
   * 
   * @return the quote start
   */
  public final java.lang.String getQuoteStart() {
    return this.quoteStart;
  }

  /**
   * Gets the quote start length.
   * 
   * @return the quote start length
   */
  public final int getQuoteStartLength() {
    return this.quoteStartLength;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "data type = " + this.DataType.getCanonicalName() + ", max length = " + this.MaxLength
        + ", delimeter = " + this.Delimiter + ", fixed width = " + this.FixedLength
        + ", format string = " + this.FormatString + ", read order = " + this.ReadOrder
        + ", read sequence = " + this.ReadOrderSequence + ", default value = " + this.DefaultValue
        + ", null if = " + this.NullIf + ", trim = " + this.TrimValue + ", object type = "
        + EngineConstants.resolveObjectIDToName(this.ObjectType);
  }

  /**
   * Sets the null if.
   * 
   * @param string the string
   */
  public void setNullIf(String string) {
    this.NullIf = string;
    if (string != null)
      this.NullIfCharArray = string.toCharArray();
  }

  public void setInternal(String internalName) {
    if (internalName == null)
      return;

    try {
      this.internal = Internal.valueOf(internalName);
    } catch (Exception e) {
      throw new IllegalArgumentException(internalName + " can be either be empty or "
          + java.util.Arrays.toString(Internal.values()));
    }
  }

  public boolean hasInternal() {
    return this.internal != null;
  }
}
