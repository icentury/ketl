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
package com.kni.etl;

import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (3/26/2002 1:29:29 PM)
 * 
 * @author: Administrator
 */
public class DestinationFieldDefinition {

	/** The Max length. */
	public int MaxLength = -1;

	/** The Fixed width. */
	public boolean FixedWidth = false;

	/** The Delimiter. */
	public String Delimiter;

	/** The Format string. */
	public String FormatString = null;

	/** The Default value. */
	public String DefaultValue;

	/** The Append line feed. */
	public boolean AppendLineFeed = false;

	// cached members
	/** The s simple date formatter. */
	private SimpleDateFormat sSimpleDateFormatter = null;

	/** The s decimal formatter. */
	private DecimalFormat sDecimalFormatter = null;

	/** The b delimiter. */
	private byte[] bDelimiter = null;

	/** The b defeault value. */
	private byte[] bDefeaultValue = null;

	/** The char set. */
	private String mCharSet = null;

	public String alwaysEscape = null;

	public boolean fileNamePort = false;

	public boolean skip = false;

	public boolean subPartitionPort = false;

	public String quoteString = null;

	public Boolean quoteEnabled;

	/**
	 * SourceFieldDefinition constructor comment.
	 * 
	 * @param pCharSet
	 *            TODO
	 */
	public DestinationFieldDefinition(String pCharSet) {
		super();
		this.mCharSet = pCharSet;
	}

	/**
	 * Gets the delimiter as bytes.
	 * 
	 * @return the delimiter as bytes
	 * 
	 * @throws UnsupportedEncodingException
	 *             the unsupported encoding exception
	 */
	public byte[] getDelimiterAsBytes() throws UnsupportedEncodingException {
		if ((this.bDelimiter == null) && (this.Delimiter != null)) {
			this.bDelimiter = this.mCharSet == null ? this.Delimiter.getBytes() : this.Delimiter
					.getBytes(this.mCharSet);
		}

		return this.bDelimiter;
	}

	/**
	 * Gets the default value as bytes.
	 * 
	 * @return the default value as bytes
	 * 
	 * @throws UnsupportedEncodingException
	 *             the unsupported encoding exception
	 */
	public byte[] getDefaultValueAsBytes() throws UnsupportedEncodingException {
		if ((this.bDefeaultValue == null) && (this.DefaultValue != null)) {
			this.bDefeaultValue = this.mCharSet == null ? this.DefaultValue.getBytes() : this.DefaultValue
					.getBytes(this.mCharSet);
		}

		return this.bDefeaultValue;
	}

	/**
	 * Gets the simple date format.
	 * 
	 * @return the simple date format
	 */
	public SimpleDateFormat getSimpleDateFormat() {
		if (this.sSimpleDateFormatter == null) {
			this.sSimpleDateFormatter = new SimpleDateFormat();
		}

		if (this.FormatString != null) {
			this.sSimpleDateFormatter.applyPattern(this.FormatString);
		}

		return (this.sSimpleDateFormatter);
	}

	/**
	 * Gets the decimal format.
	 * 
	 * @return the decimal format
	 */
	public DecimalFormat getDecimalFormat() {
		if (this.sDecimalFormatter == null) {
			this.sDecimalFormatter = new DecimalFormat();
		}

		if (this.FormatString != null) {
			this.sDecimalFormatter.applyPattern(this.FormatString);
		}

		return (this.sDecimalFormatter);
	}

	/**
	 * Gets the format.
	 * 
	 * @param cl
	 *            the cl
	 * 
	 * @return the format
	 */
	public Format getFormat(Class cl) {
		if (Number.class.isAssignableFrom(cl))
			return this.getDecimalFormat();

		if (java.util.Date.class.isAssignableFrom(cl))
			return this.getSimpleDateFormat();

		return null;

	}
}
