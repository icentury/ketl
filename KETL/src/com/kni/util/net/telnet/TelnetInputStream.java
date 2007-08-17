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
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;


/***
 *
 * <p>
 *
 * <p>
 * <p>
 * @author Daniel F. Savarese
 ***/
final class TelnetInputStream extends BufferedInputStream implements Runnable
{
    static final int _STATE_DATA = 0;
    static final int _STATE_IAC = 1;
    static final int _STATE_WILL = 2;
    static final int _STATE_WONT = 3;
    static final int _STATE_DO = 4;
    static final int _STATE_DONT = 5;
    static final int _STATE_SB = 6;
    static final int _STATE_SE = 7;
    static final int _STATE_CR = 8;
    private boolean __hasReachedEOF;
    private boolean __isClosed;
    private boolean __readIsWaiting;
    private int __receiveState;
    private int __queueHead;
    private int __queueTail;
    private int __bytesAvailable;
    private int[] __queue;
    private TelnetClient __client;
    private Thread __thread;
    private IOException __ioException;

    TelnetInputStream(InputStream input, TelnetClient client)
    {
        super(input);
        this.__client = client;
        this.__receiveState = TelnetInputStream._STATE_DATA;
        this.__isClosed = true;
        this.__hasReachedEOF = false;

        // Make it 1025, because when full, one slot will go unused, and we
        // want a 1024 byte buffer just to have a round number (base 2 that is)
        //__queue         = new int[1025];
        this.__queue = new int[2049];
        this.__queueHead = 0;
        this.__queueTail = 0;
        this.__bytesAvailable = 0;
        this.__ioException = null;
        this.__readIsWaiting = false;
        this.__thread = new Thread(this);
    }

    void _start()
    {
        int priority;
        this.__isClosed = false;

        // Need to set a higher priority in case JVM does not use pre-emptive
        // threads.  This should prevent scheduler induced deadlock (rather than
        // deadlock caused by a bug in this code).
        priority = Thread.currentThread().getPriority() + 1;

        if (priority > Thread.MAX_PRIORITY)
        {
            priority = Thread.MAX_PRIORITY;
        }

        this.__thread.setPriority(priority);
        this.__thread.setDaemon(true);
        this.__thread.start();
    }

    // synchronized(__client) critical sections are to protect against
    // TelnetOutputStream writing through the telnet client at same time
    // as a processDo/Will/etc. command invoked from TelnetInputStream
    // tries to write.
    @SuppressWarnings("unused")
    private int __read() throws IOException
    {
        int ch;

_loop: 
        while (true)
        {
            // Exit only when we reach end of stream.
            if ((ch = super.read()) < 0)
            {
                return -1;
            }

            ch = (ch & 0xff);

_mainSwitch: 
            switch (this.__receiveState)
            {
            case _STATE_CR:

                if (ch == '\0')
                {
                    // Strip null
                    continue;
                }

            // How do we handle newline after cr?
            //  else if (ch == '\n' && _requestedDont(TelnetOption.ECHO) &&
            // Handle as normal data by falling through to _STATE_DATA case
            case _STATE_DATA:

                if (ch == TelnetCommand.IAC)
                {
                    this.__receiveState = TelnetInputStream._STATE_IAC;

                    continue;
                }

                if (ch == '\r')
                {
                    synchronized (this.__client)
                    {
                        if (this.__client._requestedDont(TelnetOption.BINARY))
                        {
                            this.__receiveState = TelnetInputStream._STATE_CR;
                        }
                        else
                        {
                            this.__receiveState = TelnetInputStream._STATE_DATA;
                        }
                    }
                }
                else
                {
                    this.__receiveState = TelnetInputStream._STATE_DATA;
                }

                break;

            case _STATE_IAC:

                switch (ch)
                {
                case TelnetCommand.WILL:
                    this.__receiveState = TelnetInputStream._STATE_WILL;

                    continue;

                case TelnetCommand.WONT:
                    this.__receiveState = TelnetInputStream._STATE_WONT;

                    continue;

                case TelnetCommand.DO:
                    this.__receiveState = TelnetInputStream._STATE_DO;

                    continue;

                case TelnetCommand.DONT:
                    this.__receiveState = TelnetInputStream._STATE_DONT;

                    continue;

                case TelnetCommand.IAC:
                    this.__receiveState = TelnetInputStream._STATE_DATA;

                    break;

                default:
                    break;
                }

                this.__receiveState = TelnetInputStream._STATE_DATA;

                continue;

            case _STATE_WILL:

                synchronized (this.__client)
                {
                    this.__client._processWill(ch);
                    this.__client._flushOutputStream();
                }

                this.__receiveState = TelnetInputStream._STATE_DATA;

                continue;

            case _STATE_WONT:

                synchronized (this.__client)
                {
                    this.__client._processWont(ch);
                    this.__client._flushOutputStream();
                }

                this.__receiveState = TelnetInputStream._STATE_DATA;

                continue;

            case _STATE_DO:

                synchronized (this.__client)
                {
                    this.__client._processDo(ch);
                    this.__client._flushOutputStream();
                }

                this.__receiveState = TelnetInputStream._STATE_DATA;

                continue;

            case _STATE_DONT:

                synchronized (this.__client)
                {
                    this.__client._processDont(ch);
                    this.__client._flushOutputStream();
                }

                this.__receiveState = TelnetInputStream._STATE_DATA;

                continue;
            }

            break;
        }

        return ch;
    }

    @Override
    public int read() throws IOException
    {
        // Critical section because we're altering __bytesAvailable,
        // __queueHead, and the contents of _queue in addition to
        // testing value of __hasReachedEOF.
        synchronized (this.__queue)
        {
            while (true)
            {
                if (this.__ioException != null)
                {
                    IOException e;
                    e = this.__ioException;
                    this.__ioException = null;
                    throw e;
                }

                if (this.__bytesAvailable == 0)
                {
                    // Return -1 if at end of file
                    if (this.__hasReachedEOF)
                    {
                        return -1;
                    }

                    // Otherwise, we have to wait for queue to get something
                    this.__queue.notify();

                    try
                    {
                        this.__readIsWaiting = true;
                        this.__queue.wait();
                        this.__readIsWaiting = false;
                    }
                    catch (InterruptedException e)
                    {
                        throw new IOException(
                            "Fatal thread interruption during read.");
                    }

                    continue;
                }

                int ch;

                ch = this.__queue[this.__queueHead];

                if (++this.__queueHead >= this.__queue.length)
                {
                    this.__queueHead = 0;
                }

                --this.__bytesAvailable;

                return ch;
            }
        }
    }

    /***
     * Reads the next number of bytes from the stream into an array and
     * returns the number of bytes read.  Returns -1 if the end of the
     * stream has been reached.
     * <p>
     * @param buffer  The byte array in which to store the data.
     * @return The number of bytes read. Returns -1 if the
     *          end of the message has been reached.
     * @exception IOException If an error occurs in reading the underlying
     *            stream.
     ***/
    @Override
    public int read(byte[] buffer) throws IOException
    {
        return this.read(buffer, 0, buffer.length);
    }

    /***
     * Reads the next number of bytes from the stream into an array and returns
     * the number of bytes read.  Returns -1 if the end of the
     * message has been reached.  The characters are stored in the array
     * starting from the given offset and up to the length specified.
     * <p>
     * @param buffer The byte array in which to store the data.
     * @param offset  The offset into the array at which to start storing data.
     * @param length   The number of bytes to read.
     * @return The number of bytes read. Returns -1 if the
     *          end of the stream has been reached.
     * @exception IOException If an error occurs while reading the underlying
     *            stream.
     ***/
    @Override
    public int read(byte[] buffer, int offset, int length)
        throws IOException
    {
        int ch;
        int off;

        if (length < 1)
        {
            return 0;
        }

        // Critical section because run() may change __bytesAvailable
        synchronized (this.__queue)
        {
            if (length > this.__bytesAvailable)
            {
                length = this.__bytesAvailable;
            }
        }

        if ((ch = this.read()) == -1)
        {
            return -1;
        }

        off = offset;

        do
        {
            buffer[offset++] = (byte) ch;
        }
        while ((--length > 0) && ((ch = this.read()) != -1));

        return (offset - off);
    }

    /*** Returns false.  Mark is not supported. ***/
    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public int available() throws IOException
    {
        // Critical section because run() may change __bytesAvailable
        synchronized (this.__queue)
        {
            return this.__bytesAvailable;
        }
    }

    // Cannot be synchronized.  Will cause deadlock if run() is blocked
    // in read because BufferedInputStream read() is synchronized.
    @Override
    public void close() throws IOException
    {
        // Completely disregard the fact thread may still be running.
        // We can't afford to block on this close by waiting for
        // thread to terminate because few if any JVM's will actually
        // interrupt a system read() from the interrupt() method.
        super.close();

        synchronized (this.__queue)
        {
            this.__hasReachedEOF = true;
            this.__isClosed = true;

            if (this.__thread.isAlive())
            {
                this.__thread.interrupt();
            }

            this.__queue.notifyAll();
        }
    }

    public void run()
    {
        int ch;

        try
        {
_outerLoop: 
            while (!this.__isClosed)
            {
                try
                {
                    if ((ch = this.__read()) < 0)
                    {
                        break;
                    }
                }
                catch (InterruptedIOException e)
                {
                    synchronized (this.__queue)
                    {
                        this.__ioException = e;
                        this.__queue.notifyAll();

                        try
                        {
                            this.__queue.wait(100);
                        }
                        catch (InterruptedException interrupted)
                        {
                            if (this.__isClosed)
                            {
                                break _outerLoop;
                            }
                        }

                        continue;
                    }
                }

                // Critical section because we're altering __bytesAvailable,
                // __queueTail, and the contents of _queue.
                synchronized (this.__queue)
                {
                    while (this.__bytesAvailable >= (this.__queue.length - 1))
                    {
                        this.__queue.notify();

                        try
                        {
                            this.__queue.wait();
                        }
                        catch (InterruptedException e)
                        {
                            if (this.__isClosed)
                            {
                                break _outerLoop;
                            }
                        }
                    }

                    // Need to do this in case we're not full, but block on a read
                    if (this.__readIsWaiting)
                    {
                        this.__queue.notify();
                    }

                    this.__queue[this.__queueTail] = ch;
                    ++this.__bytesAvailable;

                    if (++this.__queueTail >= this.__queue.length)
                    {
                        this.__queueTail = 0;
                    }
                }
            }
        }
        catch (IOException e)
        {
            synchronized (this.__queue)
            {
                this.__ioException = e;
            }
        }

        synchronized (this.__queue)
        {
            this.__isClosed = true; // Possibly redundant
            this.__hasReachedEOF = true;
            this.__queue.notify();
        }
    }
}
