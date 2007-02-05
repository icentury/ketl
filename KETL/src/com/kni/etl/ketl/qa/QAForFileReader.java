/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Nov 18, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl.qa;

import java.util.ArrayList;

import com.kni.etl.SourceFieldDefinition;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public interface QAForFileReader {

    public static String QA_SIZE_CLASSNAME = "com.kni.etl.ketl.qa.QAFileSize";
    public static String QA_STRUCTURE_CLASSNAME = null;
    public static String QA_AMOUNT_CLASSNAME = "com.kni.etl.ketl.qa.QAFileAmount";
    public static String QA_VALUE_CLASSNAME = "com.kni.etl.ketl.qa.QAValue";
    public static String QA_AGE_CLASSNAME = "com.kni.etl.ketl.qa.QAFileAge";
    public static String QA_ITEM_CHECK_CLASSNAME = "com.kni.etl.ketl.qa.QAItem";
    public static String QA_RECORD_CHECK_CLASSNAME = "com.kni.etl.ketl.qa.QARecord";

    public abstract ArrayList getOpenFiles();

    // Returns the number of actually opened paths...

    public abstract String getCharacterSet();

    /**
     * @return
     */
    public abstract int getSkipLines();

    /**
     * @return
     */
    public abstract SourceFieldDefinition[] getSourceFieldDefinition();

    /**
     * @return
     */
    public abstract String getDefaultFieldDelimeter();

    /**
     * @return
     */
    public abstract char getDefaultRecordDelimter();

    /**
     * @return
     */
    public abstract boolean ignoreLastRecord();

}
