package com.kni.util.net;

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
import java.net.DatagramPacket;
import java.net.InetAddress;

/***
 * The DaytimeUDPClient class is a UDP implementation of a client for the
 * Daytime protocol described in RFC 867.  To use the class, merely
 * open a local datagram socket with
 * <a href="org.apache.commons.net.DatagramSocketClient.html#open"> open </a>
 * and call <a href="#getTime"> getTime </a> to retrieve the daytime
 * string, then
 * call <a href="org.apache.commons.net.DatagramSocketClient.html#close"> close </a>
 * to close the connection properly.  Unlike
 * <a href="org.apache.commons.net.DaytimeTCPClient.html"> DaytimeTCPClient </a>,
 * successive calls to <a href="#getTime"> getTime </a> are permitted
 * without re-establishing a connection.  That is because UDP is a
 * connectionless protocol and the Daytime protocol is stateless.
 * <p>
 * <p>
 * @author Daniel F. Savarese
 * @see DaytimeTCPClient
 ***/

public final class DaytimeUDPClient extends DatagramSocketClient
{
    /*** The default daytime port.  It is set to 13 according to RFC 867. ***/
    public static final int DEFAULT_PORT = 13;

    private byte[] __dummyData = new byte[1];
    // Received dates should be less than 256 bytes
    private byte[] __timeData = new byte[256];

    /***
     * Retrieves the time string from the specified server and port and
     * returns it.
     * <p>
     * @param host The address of the server.
     * @param port The port of the service.
     * @return The time string.
     * @exception IOException If an error occurs while retrieving the time.
     ***/
    public String getTime(InetAddress host, int port) throws IOException
    {
        DatagramPacket sendPacket, receivePacket;

        sendPacket =
            new DatagramPacket(this.__dummyData, this.__dummyData.length, host, port);
        receivePacket = new DatagramPacket(this.__timeData, this.__timeData.length);

        this._socket_.send(sendPacket);
        this._socket_.receive(receivePacket);

        return new String(receivePacket.getData(), 0, receivePacket.getLength());
    }

    /*** Same as <code>getTime(host, DaytimeUDPClient.DEFAULT_PORT);</code> ***/
    public String getTime(InetAddress host) throws IOException
    {
        return this.getTime(host, DaytimeUDPClient.DEFAULT_PORT);
    }

}

