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
package com.kni.etl.ketl.smp;

import com.kni.etl.ketl.exceptions.KETLTransformException;

// TODO: Auto-generated Javadoc
/**
 * The Interface DefaultMergeCore.
 */
public interface DefaultMergeCore extends DefaultCore {

    /** The Constant SUCCESS_ADVANCE_LEFT. */
    public final static int SUCCESS_ADVANCE_LEFT = 2;
    
    /** The Constant SUCCESS_ADVANCE_RIGHT. */
    public final static int SUCCESS_ADVANCE_RIGHT = 3;
    
    /** The Constant SUCCESS_ADVANCE_BOTH. */
    public final static int SUCCESS_ADVANCE_BOTH = 4;
    
    /** The Constant SKIP_ADVANCE_LEFT. */
    public final static int SKIP_ADVANCE_LEFT = 5;
    
    /** The Constant SKIP_ADVANCE_RIGHT. */
    public final static int SKIP_ADVANCE_RIGHT = 6;
    
    /** The Constant SKIP_ADVANCE_BOTH. */
    public final static int SKIP_ADVANCE_BOTH = 7;

    /**
     * Merge record.
     * 
     * @param pLeftInputRecord the left input record
     * @param pLeftInputDataTypes the left input data types
     * @param pLeftInputRecordWidth the left input record width
     * @param pRightInputRecord the right input record
     * @param pRightInputDataTypes the right input data types
     * @param pRightInputRecordWidth the right input record width
     * @param pOutputRecord the output record
     * @param pOutputDataTypes the output data types
     * @param pOutputRecordWidth the output record width
     * 
     * @return the int
     * 
     * @throws KETLTransformException the KETL transform exception
     */
    public int mergeRecord(Object[] pLeftInputRecord, Class[] pLeftInputDataTypes, int pLeftInputRecordWidth,
            Object[] pRightInputRecord, Class[] pRightInputDataTypes, int pRightInputRecordWidth,
            Object[] pOutputRecord, Class[] pOutputDataTypes, int pOutputRecordWidth) throws KETLTransformException;
}
