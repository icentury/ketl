package com.kni.util.net.telnet;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Commons" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.io.OutputStream;

/***
 *
 * <p>
 *
 * <p>
 * <p>
 * @author Daniel F. Savarese
 ***/


final class TelnetOutputStream extends OutputStream
{
    private TelnetClient __client;
    private boolean __convertCRtoCRLF = true;
    private boolean __lastWasCR = false;

    TelnetOutputStream(TelnetClient client)
    {
        this.__client = client;
    }


    /***
     * Writes a byte to the stream.
     * <p>
     * @param ch The byte to write.
     * @exception IOException If an error occurs while writing to the underlying
     *            stream.
     ***/
    @Override
    public void write(int ch) throws IOException
    {

        synchronized (this.__client)
        {
            ch &= 0xff;

            if (this.__client._requestedWont(TelnetOption.BINARY))
            {
                if (this.__lastWasCR)
                {
                    if (this.__convertCRtoCRLF)
                    {
                        this.__client._sendByte('\n');
                        if (ch == '\n')
                        {
                            this.__lastWasCR = false;
                            return ;
                        }
                    }
                    else if (ch != '\n')
                        this.__client._sendByte('\0');
                }

                this.__lastWasCR = false;

                switch (ch)
                {
                case '\r':
                    this.__client._sendByte('\r');
                    this.__lastWasCR = true;
                    break;
                case TelnetCommand.IAC:
                    this.__client._sendByte(TelnetCommand.IAC);
                    this.__client._sendByte(TelnetCommand.IAC);
                    break;
                default:
                    this.__client._sendByte(ch);
                    break;
                }
            }
            else if (ch == TelnetCommand.IAC)
            {
                this.__client._sendByte(ch);
                this.__client._sendByte(TelnetCommand.IAC);
            }
            else
                this.__client._sendByte(ch);
        }
    }


    /***
     * Writes a byte array to the stream.
     * <p>
     * @param buffer  The byte array to write.
     * @exception IOException If an error occurs while writing to the underlying
     *            stream.
     ***/
    @Override
    public void write(byte buffer[]) throws IOException
    {
        this.write(buffer, 0, buffer.length);
    }


    /***
     * Writes a number of bytes from a byte array to the stream starting from
     * a given offset.
     * <p>
     * @param buffer  The byte array to write.
     * @param offset  The offset into the array at which to start copying data.
     * @param length  The number of bytes to write.
     * @exception IOException If an error occurs while writing to the underlying
     *            stream.
     ***/
    @Override
    public void write(byte buffer[], int offset, int length) throws IOException
    {
        synchronized (this.__client)
        {
            while (length-- > 0)
                this.write(buffer[offset++]);
        }
    }

    /*** Flushes the stream. ***/
    @Override
    public void flush() throws IOException
    {
        this.__client._flushOutputStream();
    }

    /*** Closes the stream. ***/
    @Override
    public void close() throws IOException
    {
        this.__client._closeOutputStream();
    }
}
