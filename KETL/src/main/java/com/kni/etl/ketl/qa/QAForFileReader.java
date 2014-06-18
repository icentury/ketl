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
/*
 * Created on Nov 18, 2004
 * 
 * To change the template for this generated file go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
package com.kni.etl.ketl.qa;

import java.util.List;

import com.kni.etl.SourceFieldDefinition;

// TODO: Auto-generated Javadoc
/**
 * The Interface QAForFileReader.
 * 
 * @author nwakefield To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public interface QAForFileReader {

  /** The Q a_ SIZ e_ CLASSNAME. */
  public static String QA_SIZE_CLASSNAME = "com.kni.etl.ketl.qa.QAFileSize";

  /** The Q a_ STRUCTUR e_ CLASSNAME. */
  public static String QA_STRUCTURE_CLASSNAME = null;

  /** The Q a_ AMOUN t_ CLASSNAME. */
  public static String QA_AMOUNT_CLASSNAME = "com.kni.etl.ketl.qa.QAFileAmount";

  /** The Q a_ VALU e_ CLASSNAME. */
  public static String QA_VALUE_CLASSNAME = "com.kni.etl.ketl.qa.QAValue";

  /** The Q a_ AG e_ CLASSNAME. */
  public static String QA_AGE_CLASSNAME = "com.kni.etl.ketl.qa.QAFileAge";

  /** The Q a_ ITE m_ CHEC k_ CLASSNAME. */
  public static String QA_ITEM_CHECK_CLASSNAME = "com.kni.etl.ketl.qa.QAItem";

  /** The Q a_ RECOR d_ CHEC k_ CLASSNAME. */
  public static String QA_RECORD_CHECK_CLASSNAME = "com.kni.etl.ketl.qa.QARecord";

  /**
   * Gets the open files.
   * 
   * @return the open files
   */
  public abstract List getOpenFiles();

  // Returns the number of actually opened paths...

  /**
   * Gets the character set.
   * 
   * @return the character set
   */
  public abstract String getCharacterSet();

  /**
   * Gets the skip lines.
   * 
   * @return the skip lines
   */
  public abstract int getSkipLines();

  /**
   * Gets the source field definition.
   * 
   * @return the source field definition
   */
  public abstract SourceFieldDefinition[] getSourceFieldDefinition();

  /**
   * Gets the default field delimeter.
   * 
   * @return the default field delimeter
   */
  public abstract String getDefaultFieldDelimeter();

  /**
   * Gets the default record delimter.
   * 
   * @return the default record delimter
   */
  public abstract char getDefaultRecordDelimter();

  /**
   * Ignore last record.
   * 
   * @return true, if successful
   */
  public abstract boolean ignoreLastRecord();

}
