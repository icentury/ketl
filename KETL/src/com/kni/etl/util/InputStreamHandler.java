/**
 * 
 */
package com.kni.etl.util;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamHandler extends Thread {

    static final int MAX_MESSAGE_SIZE = 1000;

    /**
     * Stream being read
     */
    private InputStream m_stream;

    /**
     * The StringBuffer holding the captured output
     */
    private StringBuffer m_captureBuffer;

    /**
     * Constructor.
     * 
     * @param executor TODO
     * @param
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