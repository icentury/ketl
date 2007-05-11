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

import java.io.IOException;
import java.io.InputStream;

// TODO: Auto-generated Javadoc
/**
 * The Class InputStreamHandler.
 */
public class InputStreamHandler extends Thread {

    /** The Constant MAX_MESSAGE_SIZE. */
    static final int MAX_MESSAGE_SIZE = 1000;

    /** Stream being read. */
    private InputStream m_stream;

    /** The StringBuffer holding the captured output. */
    private StringBuffer m_captureBuffer;

    /**
     * Constructor.
     * 
     * @param captureBuffer the capture buffer
     * @param stream the stream
     */
    public InputStreamHandler(StringBuffer captureBuffer, InputStream stream) {
        this.m_stream = stream;
        this.m_captureBuffer = captureBuffer;
        this.start();
    }

    /**
     * Stream the data.
     */
    @Override
    public void run() {
        try {
            int nextChar;

            while ((nextChar = this.m_stream.read()) != -1) {
                this.m_captureBuffer.append((char) nextChar);

                if (this.m_captureBuffer.length() == InputStreamHandler.MAX_MESSAGE_SIZE) {
                    this.m_captureBuffer.append("\n[Max message size reached trimming]");

                    while ((nextChar = this.m_stream.read()) != -1) {
                    }

                    ;

                    return;
                }
            }
        } catch (IOException ioe) {
        }
    }
}