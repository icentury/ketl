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
package com.kni.etl.util;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import com.kni.etl.FieldLevelFastInputChannel;

// TODO: Auto-generated Javadoc
/**
 * The Class ManagedFastInputChannel.
 */
public class ManagedFastInputChannel {

    /** The mf channel. */
    public ReadableByteChannel mfChannel;
    
    /** The mi current record. */
    public int miCurrentRecord;
    
    /** The path. */
    public String mPath;
    
    /** The reader. */
    public FieldLevelFastInputChannel mReader;

	public File file;

    /**
     * Close.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void close() throws IOException {
        this.mfChannel.close();
        if (this.mReader != null)
            this.mReader.close();
    }
}