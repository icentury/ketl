package com.kni.util.net.io;


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
import java.io.PushbackReader;
import java.io.Reader;


/**
 * DotTerminatedMessageReader is a class used to read messages from a
 * server that are terminated by a single dot followed by a
 * &lt;CR&gt;&lt;LF&gt;
 * sequence and with double dots appearing at the begining of lines which
 * do not signal end of message yet start with a dot.  Various Internet
 * protocols such as NNTP and POP3 produce messages of this type.
 * <p>
 * This class handles stripping of the duplicate period at the beginning
 * of lines starting with a period, converts NETASCII newlines to the
 * local line separator format, truncates the end of message indicator,
 * and ensures you cannot read past the end of the message.
 * @author <a href="mailto:savarese@apache.org">Daniel F. Savarese</a>
 * @version $Id: DotTerminatedMessageReader.java,v 1.1 2006/12/13 07:06:49 nwakefield Exp $
 */
public final class DotTerminatedMessageReader extends Reader
{
    private static final String LS;
    private static final char[] LS_CHARS;

    static
    {
        LS = System.getProperty("line.separator");
        LS_CHARS = DotTerminatedMessageReader.LS.toCharArray();
    }

    private boolean atBeginning;
    private boolean eof;
    private int pos;
    private char[] internalBuffer;
    private PushbackReader internalReader;

    /**
     * Creates a DotTerminatedMessageReader that wraps an existing Reader
     * input source.
     * @param reader  The Reader input source containing the message.
     */
    public DotTerminatedMessageReader(Reader reader)
    {
        super(reader);
        this.internalBuffer = new char[DotTerminatedMessageReader.LS_CHARS.length + 3];
        this.pos = this.internalBuffer.length;

        // Assumes input is at start of message
        this.atBeginning = true;
        this.eof = false;
        this.internalReader = new PushbackReader(reader);
    }

    /**
     * Reads and returns the next character in the message.  If the end of the
     * message has been reached, returns -1.  Note that a call to this method
     * may result in multiple reads from the underlying input stream to decode
     * the message properly (removing doubled dots and so on).  All of
     * this is transparent to the programmer and is only mentioned for
     * completeness.
     * @return The next character in the message. Returns -1 if the end of the
     *          message has been reached.
     * @exception IOException If an error occurs while reading the underlying
     *            stream.
     */
    @Override
    public int read() throws IOException
    {
        int ch;

        synchronized (this.lock)
        {
            if (this.pos < this.internalBuffer.length)
            {
                return this.internalBuffer[this.pos++];
            }

            if (this.eof)
            {
                return -1;
            }

            if ((ch = this.internalReader.read()) == -1)
            {
                this.eof = true;

                return -1;
            }

            if (this.atBeginning)
            {
                this.atBeginning = false;

                if (ch == '.')
                {
                    ch = this.internalReader.read();

                    if (ch != '.')
                    {
                        // read newline
                        this.eof = true;
                        this.internalReader.read();

                        return -1;
                    }

                    return '.';
                }
            }

            if (ch == '\r')
            {
                ch = this.internalReader.read();

                if (ch == '\n')
                {
                    ch = this.internalReader.read();

                    if (ch == '.')
                    {
                        ch = this.internalReader.read();

                        if (ch != '.')
                        {
                            // read newline and indicate end of file
                            this.internalReader.read();
                            this.eof = true;
                        }
                        else
                        {
                            this.internalBuffer[--this.pos] = (char) ch;
                        }
                    }
                    else
                    {
                        this.internalReader.unread(ch);
                    }

                    this.pos -= DotTerminatedMessageReader.LS_CHARS.length;
                    System.arraycopy(DotTerminatedMessageReader.LS_CHARS, 0, this.internalBuffer, this.pos,
                        DotTerminatedMessageReader.LS_CHARS.length);
                    ch = this.internalBuffer[this.pos++];
                }
                else
                {
                    this.internalBuffer[--this.pos] = (char) ch;

                    return '\r';
                }
            }

            return ch;
        }
    }

    /**
     * Reads the next characters from the message into an array and
     * returns the number of characters read.  Returns -1 if the end of the
     * message has been reached.
     * @param buffer  The character array in which to store the characters.
     * @return The number of characters read. Returns -1 if the
     *          end of the message has been reached.
     * @exception IOException If an error occurs in reading the underlying
     *            stream.
     */
    @Override
    public int read(char[] buffer) throws IOException
    {
        return this.read(buffer, 0, buffer.length);
    }

    /**
     * Reads the next characters from the message into an array and
     * returns the number of characters read.  Returns -1 if the end of the
     * message has been reached.  The characters are stored in the array
     * starting from the given offset and up to the length specified.
     * @param buffer  The character array in which to store the characters.
     * @param offset   The offset into the array at which to start storing
     *              characters.
     * @param length   The number of characters to read.
     * @return The number of characters read. Returns -1 if the
     *          end of the message has been reached.
     * @exception IOException If an error occurs in reading the underlying
     *            stream.
     */
    @Override
    public int read(char[] buffer, int offset, int length)
        throws IOException
    {
        int ch;
        int off;

        synchronized (this.lock)
        {
            if (length < 1)
            {
                return 0;
            }

            if ((ch = this.read()) == -1)
            {
                return -1;
            }

            off = offset;

            do
            {
                buffer[offset++] = (char) ch;
            }
            while ((--length > 0) && ((ch = this.read()) != -1));

            return (offset - off);
        }
    }

    /**
     * Determines if the message is ready to be read.
     * @return True if the message is ready to be read, false if not.
     * @exception IOException If an error occurs while checking the underlying
     *            stream.
     */
    @Override
    public boolean ready() throws IOException
    {
        synchronized (this.lock)
        {
            return ((this.pos < this.internalBuffer.length) || this.internalReader.ready());
        }
    }

    /**
     * Closes the message for reading.  This doesn't actually close the
     * underlying stream.  The underlying stream may still be used for
     * communicating with the server and therefore is not closed.
     * <p>
     * If the end of the message has not yet been reached, this method
     * will read the remainder of the message until it reaches the end,
     * so that the underlying stream may continue to be used properly
     * for communicating with the server.  If you do not fully read
     * a message, you MUST close it, otherwise your program will likely
     * hang or behave improperly.
     * @exception IOException  If an error occurs while reading the
     *            underlying stream.
     */
    @Override
    public void close() throws IOException
    {
        synchronized (this.lock)
        {
            if (this.internalReader == null)
            {
                return;
            }

            if (!this.eof)
            {
                while (this.read() != -1)
                {
                    ;
                }
            }

            this.eof = true;
            this.atBeginning = false;
            this.pos = this.internalBuffer.length;
            this.internalReader = null;
        }
    }
}
