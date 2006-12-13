/**
 * 
 */
package com.kni.etl.util;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamHandler extends Thread
{
  
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
     * @param executor TODO
     *
     * @param
     */
    public InputStreamHandler( StringBuffer captureBuffer, InputStream stream)
    {
        m_stream = stream;
        m_captureBuffer = captureBuffer;
        start();
    }

    /**
     * Stream the data.
     */
    public void run()
    {
        try
        {
            int nextChar;

            while ((nextChar = m_stream.read()) != -1)
            {
                m_captureBuffer.append((char) nextChar);

                if (m_captureBuffer.length() == MAX_MESSAGE_SIZE)
                {
                    m_captureBuffer.append("\n[Max message size reached trimming]");

                    while ((nextChar = m_stream.read()) != -1)
                    {
                    }

                    ;

                    return;
                }
            }
        }
        catch (IOException ioe)
        {
        }
    }
}