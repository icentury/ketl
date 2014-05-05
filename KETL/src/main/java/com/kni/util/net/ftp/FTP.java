package com.kni.util.net.ftp;

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.Vector;

import com.kni.util.net.MalformedServerReplyException;
import com.kni.util.net.ProtocolCommandListener;
import com.kni.util.net.ProtocolCommandSupport;
import com.kni.util.net.SocketClient;
import com.kni.util.net.telnet.TelnetClient;

/***
 * FTP provides the basic the functionality necessary to implement your
 * own FTP client.  It extends org.apache.commons.net.TelnetClient
 * simply because it saves the writing of extra code to handle the FTP
 * control connection which always remains open during an FTP session and
 * uses the Telnet protocol.  Aggregation would require writing new
 * wrapper methods and wouldn't leverage the functionality already
 * present in org.apache.commons.net.SocketClient.
 * <p>
 * To derive the full benefits of the FTP class requires some knowledge
 * of the FTP protocol defined in RFC 959.  However, there is no reason
 * why you should have to use the FTP class.  The
 * <a href="org.apache.commons.net.ftp.FTPClient.html"> FTPClient </a> class,
 * derived from FTP,
 * implements all the functionality required of an FTP client.  The
 * FTP class is made public to provide access to various FTP constants
 * and to make it easier for adventurous programmers (or those with
 * special needs) to interact with the FTP protocol and implement their
 * own clients.  A set of methods with names corresponding to the FTP
 * command names are provided to facilitate this interaction.
 * <p>
 * You should keep in mind that the FTP server may choose to prematurely
 * close a connection if the client has been idle for longer than a
 * given time period (usually 900 seconds).  The FTP class will detect a
 * premature FTP server connection closing when it receives a
 * <a href="org.apache.commons.net.ftp.FTPReply.html#SERVICE_NOT_AVAILABLE">
 * FTPReply.SERVICE_NOT_AVAILABLE </a> response to a command.
 * When that occurs, the FTP class method encountering that reply will throw
 * an <a href="org.apache.commons.net.ftp.FTPConnectionClosedException.html">
 * FTPConnectionClosedException </a>.  <code>FTPConectionClosedException</code>
 * is a subclass of <code> IOException </code> and therefore need not be
 * caught separately, but if you are going to catch it separately, its
 * catch block must appear before the more general <code> IOException </code>
 * catch block.  When you encounter an
 * <a href="org.apache.commons.net.ftp.FTPConnectionClosedException.html">
 * FTPConnectionClosedException </a>, you must disconnect the connection with
 * <a href="#disconnect"> disconnect() </a> to properly clean up the
 * system resources used by FTP.  Before disconnecting, you may check the
 * last reply code and text with
 * <a href="#getReplyCode"> getReplyCode </a>,
 * <a href="#getReplyString"> getReplyString </a>,
 * and <a href="#getReplyStrings"> getReplyStrings</a>.
 * You may avoid server disconnections while the client is idle by
 * periodicaly sending NOOP commands to the server.
 * <p>
 * Rather than list it separately for each method, we mention here that
 * every method communicating with the server and throwing an IOException
 * can also throw a 
 * <a href="org.apache.commons.net.MalformedServerReplyException.html">
 * MalformedServerReplyException </a>, which is a subclass
 * of IOException.  A MalformedServerReplyException will be thrown when
 * the reply received from the server deviates enough from the protocol 
 * specification that it cannot be interpreted in a useful manner despite
 * attempts to be as lenient as possible.
 * <p>
 * <p>
 * @author Daniel F. Savarese
 * @see FTPClient
 * @see FTPConnectionClosedException
 * @see org.apache.commons.net.MalformedServerReplyException
 ***/

public class FTP extends TelnetClient
{
    /*** The default FTP data port (20). ***/
    public static final int DEFAULT_DATA_PORT = 20;
    /*** The default FTP control port (21). ***/
    public static final int DEFAULT_PORT = 21;

    /***
     * A constant used to indicate the file(s) being transfered should
     * be treated as ASCII.  This is the default file type.  All constants
     * ending in <code>FILE_TYPE</code> are used to indicate file types.
     ***/
    public static final int ASCII_FILE_TYPE = 0;

    /***
     * A constant used to indicate the file(s) being transfered should
     * be treated as EBCDIC.  Note however that there are several different
     * EBCDIC formats.  All constants ending in <code>FILE_TYPE</code>
     * are used to indicate file types.
     ***/
    public static final int EBCDIC_FILE_TYPE = 1;

    /***
     * A constant used to indicate the file(s) being transfered should
     * be treated as a binary image, i.e., no translations should be
     * performed.  All constants ending in <code>FILE_TYPE</code> are used to
     * indicate file types.
     ***/
    public static final int IMAGE_FILE_TYPE = 2;

    /***
     * A constant used to indicate the file(s) being transfered should
     * be treated as a binary image, i.e., no translations should be
     * performed.  All constants ending in <code>FILE_TYPE</code> are used to
     * indicate file types.
     ***/
    public static final int BINARY_FILE_TYPE = 2;

    /***
     * A constant used to indicate the file(s) being transfered should
     * be treated as a local type.  All constants ending in
     * <code>FILE_TYPE</code> are used to indicate file types.
     ***/
    public static final int LOCAL_FILE_TYPE = 3;

    /***
     * A constant used for text files to indicate a non-print text format.
     * This is the default format.
     * All constants ending in <code>TEXT_FORMAT</code> are used to indicate
     * text formatting for text transfers (both ASCII and EBCDIC).
     ***/
    public static final int NON_PRINT_TEXT_FORMAT = 4;

    /***
     * A constant used to indicate a text file contains format vertical format
     * control characters.
     * All constants ending in <code>TEXT_FORMAT</code> are used to indicate
     * text formatting for text transfers (both ASCII and EBCDIC).
     ***/
    public static final int TELNET_TEXT_FORMAT = 5;

    /***
     * A constant used to indicate a text file contains ASA vertical format
     * control characters.
     * All constants ending in <code>TEXT_FORMAT</code> are used to indicate
     * text formatting for text transfers (both ASCII and EBCDIC).
     ***/
    public static final int CARRIAGE_CONTROL_TEXT_FORMAT = 6;

    /***
     * A constant used to indicate a file is to be treated as a continuous
     * sequence of bytes.  This is the default structure.  All constants ending
     * in <code>_STRUCTURE</code> are used to indicate file structure for
     * file transfers.
     ***/
    public static final int FILE_STRUCTURE = 7;

    /***
     * A constant used to indicate a file is to be treated as a sequence
     * of records.  All constants ending in <code>_STRUCTURE</code>
     * are used to indicate file structure for file transfers.
     ***/
    public static final int RECORD_STRUCTURE = 8;

    /***
     * A constant used to indicate a file is to be treated as a set of
     * independent indexed pages.  All constants ending in
     * <code>_STRUCTURE</code> are used to indicate file structure for file
     * transfers.
     ***/
    public static final int PAGE_STRUCTURE = 9;

    /***
     * A constant used to indicate a file is to be transfered as a stream
     * of bytes.  This is the default transfer mode.  All constants ending
     * in <code>TRANSFER_MODE</code> are used to indicate file transfer
     * modes.
     ***/
    public static final int STREAM_TRANSFER_MODE = 10;

    /***
     * A constant used to indicate a file is to be transfered as a series
     * of blocks.  All constants ending in <code>TRANSFER_MODE</code> are used
     * to indicate file transfer modes.
     ***/
    public static final int BLOCK_TRANSFER_MODE = 11;

    /***
     * A constant used to indicate a file is to be transfered as FTP
     * compressed data.  All constants ending in <code>TRANSFER_MODE</code>
     * are used to indicate file transfer modes.
     ***/
    public static final int COMPRESSED_TRANSFER_MODE = 12;

    private static final String __modes = "ABILNTCFRPSBC";

    private StringBuffer __commandBuffer;

    BufferedReader _controlInput;
    BufferedWriter _controlOutput;
    int _replyCode;
    Vector _replyLines;
    boolean _newReplyString;
    String _replyString;

    /***
     * A ProtocolCommandSupport object used to manage the registering of
     * ProtocolCommandListeners and te firing of ProtocolCommandEvents.
     ***/
    protected ProtocolCommandSupport _commandSupport_;

    /***
     * The default FTP constructor.  Sets the default port to
     * <code>DEFAULT_PORT</code> and initializes internal data structures
     * for saving FTP reply information.
     ***/
    public FTP()
    {
        this.setDefaultPort(FTP.DEFAULT_PORT);
        this.__commandBuffer = new StringBuffer();
        this._replyLines = new Vector();
        this._newReplyString = false;
        this._replyString = null;
        this._commandSupport_ = new ProtocolCommandSupport(this);
    }

    private void __getReply() throws IOException
    {
        int length;
        String line, code;

        this._newReplyString = true;
        this._replyLines.setSize(0);

        line = this._controlInput.readLine();

        if (line == null)
            throw new FTPConnectionClosedException(
                "Connection closed without indication.");

        // In case we run into an anomaly we don't want fatal index exceptions
        // to be thrown.
        length = line.length();
        if (length < 3)
            throw new MalformedServerReplyException(
                "Truncated server reply: " + line);

        try
        {
			code = line.substring(0, 3);
            this._replyCode = Integer.parseInt(code);
        }
        catch (NumberFormatException e)
        {
            throw new MalformedServerReplyException(
                "Could not parse response code.\nServer Reply: " + line);
        }

        this._replyLines.addElement(line);

        // Get extra lines if message continues.
        if (length > 3 && line.charAt(3) == '-')
        {
            do
            {
                line = this._controlInput.readLine();

                if (line == null)
                    throw new FTPConnectionClosedException(
                        "Connection closed without indication.");

                this._replyLines.addElement(line);

                // The length() check handles problems that could arise from readLine()
                // returning too soon after encountering a naked CR or some other
                // anomaly.
            }
            while (!(line.length() >= 4 && line.charAt(3) != '-' &&
                     Character.isDigit(line.charAt(0))));
            // This is too strong a condition because of non-conforming ftp
            // servers like ftp.funet.fi which sent 226 as the last line of a
            // 426 multi-line reply in response to ls /.  We relax the condition to
            // test that the line starts with a digit rather than starting with
            // the code.
            // line.startsWith(code)));
        }

        if (this._commandSupport_.getListenerCount() > 0)
            this._commandSupport_.fireReplyReceived(this._replyCode, this.getReplyString());

        if (this._replyCode == FTPReply.SERVICE_NOT_AVAILABLE)
            throw new FTPConnectionClosedException(
                "FTP response 421 received.  Server closed connection.");
    }

    // initiates control connections and gets initial reply
    @Override
    protected void _connectAction_() throws IOException
    {
        super._connectAction_();
        this._controlInput =
            new BufferedReader(new InputStreamReader(this.getInputStream()));
        this._controlOutput =
            new BufferedWriter(new OutputStreamWriter(this.getOutputStream()));
        this.__getReply();
        // If we received code 120, we have to fetch completion reply.
        if (FTPReply.isPositivePreliminary(this._replyCode))
            this.__getReply();
    }


    /***
     * Adds a ProtocolCommandListener.  Delegates this task to
     * <a href="#_commandSupport_"> _commandSupport_ </a>.
     * <p>
     * @param listener  The ProtocolCommandListener to add.
     ***/
    public void addProtocolCommandListener(ProtocolCommandListener listener)
    {
        this._commandSupport_.addProtocolCommandListener(listener);
    }

    /***
     * Removes a ProtocolCommandListener.  Delegates this task to
     * <a href="#_commandSupport_"> _commandSupport_ </a>.
     * <p>
     * @param listener  The ProtocolCommandListener to remove.
     ***/
    public void removeProtocolCommandistener(ProtocolCommandListener listener)
    {
        this._commandSupport_.removeProtocolCommandListener(listener);
    }


    /***
     * Closes the control connection to the FTP server and sets to null
     * some internal data so that the memory may be reclaimed by the
     * garbage collector.  The reply text and code information from the
     * last command is voided so that the memory it used may be reclaimed.
     * <p>
     * @exception IOException If an error occurs while disconnecting.
     ***/
    @Override
    public void disconnect() throws IOException
    {
        super.disconnect();
        this._controlInput = null;
        this._controlOutput = null;
        this._replyLines.setSize(0);
        this._newReplyString = false;
        this._replyString = null;
    }


    /***
     * Sends an FTP command to the server, waits for a reply and returns the
     * numerical response code.  After invocation, for more detailed
     * information, the actual reply text can be accessed by calling
     * <a href="#getReplyString"> getReplyString </a> or 
     * <a href="#getReplyStrings"> getReplyStrings </a>.
     * <p>
     * @param command  The text representation of the  FTP command to send.
     * @param args The arguments to the FTP command.  If this parameter is
     *             set to null, then the command is sent with no argument.
     * @return The integer value of the FTP reply code returned by the server
     *         in response to the command.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int sendCommand(String command, String args) throws IOException
    {
        String message;

        this.__commandBuffer.setLength(0);
        this.__commandBuffer.append(command);

        if (args != null)
        {
            this.__commandBuffer.append(' ');
            this.__commandBuffer.append(args);
        }
        this.__commandBuffer.append(SocketClient.NETASCII_EOL);

        this._controlOutput.write(message = this.__commandBuffer.toString());
        this._controlOutput.flush();

        if (this._commandSupport_.getListenerCount() > 0)
            this._commandSupport_.fireCommandSent(command, message);

        this.__getReply();
        return this._replyCode;
    }


    /***
     * Sends an FTP command to the server, waits for a reply and returns the
     * numerical response code.  After invocation, for more detailed
     * information, the actual reply text can be accessed by calling
     * <a href="#getReplyString"> getReplyString </a> or 
     * <a href="#getReplyStrings"> getReplyStrings </a>.
     * <p>
     * @param command  The FTPCommand constant corresponding to the FTP command
     *                 to send.
     * @param args The arguments to the FTP command.  If this parameter is
     *             set to null, then the command is sent with no argument.
     * @return The integer value of the FTP reply code returned by the server
     *         in response to the command.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int sendCommand(int command, String args) throws IOException
    {
        return this.sendCommand(FTPCommand._commands[command], args);
    }


    /***
     * Sends an FTP command with no arguments to the server, waits for a
     * reply and returns the numerical response code.  After invocation, for
     * more detailed information, the actual reply text can be accessed by
     * calling <a href="#getReplyString"> getReplyString </a> or 
     * <a href="#getReplyStrings"> getReplyStrings </a>.
     * <p>
     * @param command  The text representation of the  FTP command to send.
     * @return The integer value of the FTP reply code returned by the server
     *         in response to the command.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int sendCommand(String command) throws IOException
    {
        return this.sendCommand(command, null);
    }


    /***
     * Sends an FTP command with no arguments to the server, waits for a
     * reply and returns the numerical response code.  After invocation, for
     * more detailed information, the actual reply text can be accessed by
     * calling <a href="#getReplyString"> getReplyString </a> or 
     * <a href="#getReplyStrings"> getReplyStrings </a>.
     * <p>
     * @param command  The FTPCommand constant corresponding to the FTP command
     *                 to send.
     * @return The integer value of the FTP reply code returned by the server
     *         in response to the command.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int sendCommand(int command) throws IOException
    {
        return this.sendCommand(command, null);
    }


    /***
     * Returns the integer value of the reply code of the last FTP reply.
     * You will usually only use this method after you connect to the
     * FTP server to check that the connection was successful since
     * <code> connect </code> is of type void.
     * <p>
     * @return The integer value of the reply code of the last FTP reply.
     ***/
    public int getReplyCode()
    {
        return this._replyCode;
    }

    /***
     * Fetches a reply from the FTP server and returns the integer reply
     * code.  After calling this method, the actual reply text can be accessed
     * from either  calling <a href="#getReplyString"> getReplyString </a> or 
     * <a href="#getReplyStrings"> getReplyStrings </a>.  Only use this
     * method if you are implementing your own FTP client or if you need to
     * fetch a secondary response from the FTP server.
     * <p>
     * @return The integer value of the reply code of the fetched FTP reply.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while receiving the
     *                         server reply.
     ***/
    public int getReply() throws IOException
    {
        this.__getReply();
        return this._replyCode;
    }


    /***
     * Returns the lines of text from the last FTP server response as an array
     * of strings, one entry per line.  The end of line markers of each are
     * stripped from each line.
     * <p>
     * @return The lines of text from the last FTP response as an array.
     ***/
    public String[] getReplyStrings()
    {
        String[] lines;
        lines = new String[this._replyLines.size()];
        this._replyLines.copyInto(lines);
        return lines;
    }

    /***
     * Returns the entire text of the last FTP server response exactly
     * as it was received, including all end of line markers in NETASCII
     * format.
     * <p>
     * @return The entire text from the last FTP response as a String.
     ***/
    public String getReplyString()
    {
        Enumeration enumReply;
        StringBuffer buffer;

        if (!this._newReplyString)
            return this._replyString;

        buffer = new StringBuffer(256);
        enumReply = this._replyLines.elements();
        while (enumReply.hasMoreElements())
        {
            buffer.append((String)enumReply.nextElement());
            buffer.append(SocketClient.NETASCII_EOL);
        }

        this._newReplyString = false;

        return (this._replyString = buffer.toString());
    }


    /***
     * A convenience method to send the FTP USER command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param username  The username to login under.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int user(String username) throws IOException
    {
        return this.sendCommand(FTPCommand.USER, username);
    }

    /***
     * A convenience method to send the FTP PASS command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param pass  The plain text password of the username being logged into.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int pass(String password) throws IOException
    {
        return this.sendCommand(FTPCommand.PASS, password);
    }

    /***
     * A convenience method to send the FTP ACCT command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param account  The account name to access.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int acct(String account) throws IOException
    {
        return this.sendCommand(FTPCommand.ACCT, account);
    }


    /***
     * A convenience method to send the FTP ABOR command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int abor() throws IOException
    {
        return this.sendCommand(FTPCommand.ABOR);
    }

    /***
     * A convenience method to send the FTP CWD command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param directory The new working directory.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int cwd(String directory) throws IOException
    {
        return this.sendCommand(FTPCommand.CWD, directory);
    }

    /***
     * A convenience method to send the FTP CDUP command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int cdup() throws IOException
    {
        return this.sendCommand(FTPCommand.CDUP);
    }

    /***
     * A convenience method to send the FTP QUIT command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int quit() throws IOException
    {
        return this.sendCommand(FTPCommand.QUIT);
    }

    /***
     * A convenience method to send the FTP REIN command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int rein() throws IOException
    {
        return this.sendCommand(FTPCommand.REIN);
    }

    /***
     * A convenience method to send the FTP SMNT command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param dir  The directory name.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int smnt(String dir) throws IOException
    {
        return this.sendCommand(FTPCommand.SMNT);
    }

    /***
     * A convenience method to send the FTP PORT command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param host  The host owning the port.
     * @param port  The new port.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int port(InetAddress host, int port) throws IOException
    {
        int num;
        StringBuffer info = new StringBuffer(24);

        info.append(host.getHostAddress().replace('.', ','));
        num = port >>> 8;
        info.append(',');
        info.append(num);
        info.append(',');
        num = port & 0xff;
        info.append(num);

        return this.sendCommand(FTPCommand.PORT, info.toString());
    }

    /***
     * A convenience method to send the FTP PASV command to the server,
     * receive the reply, and return the reply code.  Remember, it's up
     * to you to interpret the reply string containing the host/port
     * information.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int pasv() throws IOException
    {
        return this.sendCommand(FTPCommand.PASV);
    }

    /***
     * A convenience method to send the FTP TYPE command for text files
     * to the server, receive the reply, and return the reply code.
     * <p>
     * @param type  The type of the file (one of the <code>FILE_TYPE</code>
     *              constants).
     * @param formatOrByteSize  The format of the file (one of the
     *              <code>_FORMAT</code> constants.  In the case of
     *              <code>LOCAL_FILE_TYPE</code>, the byte size.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int type(int fileType, int formatOrByteSize) throws IOException
    {
        StringBuffer arg = new StringBuffer();

        arg.append(FTP.__modes.charAt(fileType));
        arg.append(' ');
        if (fileType == FTP.LOCAL_FILE_TYPE)
            arg.append(formatOrByteSize);
        else
            arg.append(FTP.__modes.charAt(formatOrByteSize));

        return this.sendCommand(FTPCommand.TYPE, arg.toString());
    }


    /***
     * A convenience method to send the FTP TYPE command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param type  The type of the file (one of the <code>FILE_TYPE</code>
     *              constants).
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int type(int fileType) throws IOException
    {
        return this.sendCommand(FTPCommand.TYPE,
                           FTP.__modes.substring(fileType, fileType + 1));
    }

    /***
     * A convenience method to send the FTP STRU command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param structure  The structure of the file (one of the
     *         <code>_STRUCTURE</code> constants).
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int stru(int structure) throws IOException
    {
        return this.sendCommand(FTPCommand.STRU,
                           FTP.__modes.substring(structure, structure + 1));
    }

    /***
     * A convenience method to send the FTP MODE command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param mode  The transfer mode to use (one of the
     *         <code>TRANSFER_MODE</code> constants).
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int mode(int mode) throws IOException
    {
        return this.sendCommand(FTPCommand.MODE,
                           FTP.__modes.substring(mode, mode + 1));
    }

    /***
     * A convenience method to send the FTP RETR command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @param pathname  The pathname of the file to retrieve.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int retr(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.RETR, pathname);
    }

    /***
     * A convenience method to send the FTP STOR command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @param pathname  The pathname to use for the file when stored at
     *                  the remote end of the transfer.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int stor(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.STOR, pathname);
    }

    /***
     * A convenience method to send the FTP STOU command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int stou() throws IOException
    {
        return this.sendCommand(FTPCommand.STOU);
    }

    /***
     * A convenience method to send the FTP STOU command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @param pathname  The base pathname to use for the file when stored at
     *                  the remote end of the transfer.  Some FTP servers
     *                  require this.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int stou(String filename) throws IOException
    {
        return this.sendCommand(FTPCommand.STOU, filename);
    }

    /***
     * A convenience method to send the FTP APPE command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @param pathname  The pathname to use for the file when stored at
     *                  the remote end of the transfer.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int appe(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.APPE, pathname);
    }

    /***
     * A convenience method to send the FTP ALLO command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param bytes The number of bytes to allocate.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int allo(int bytes) throws IOException
    {
        return this.sendCommand(FTPCommand.ALLO, Integer.toString(bytes));
    }

    /***
     * A convenience method to send the FTP ALLO command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param bytes The number of bytes to allocate.
     * @param recordSize  The size of a record.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int allo(int bytes, int recordSize) throws IOException
    {
        return this.sendCommand(FTPCommand.ALLO, Integer.toString(bytes) + " R " +
                           Integer.toString(recordSize));
    }

    /***
     * A convenience method to send the FTP REST command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param marker The marker at which to restart a transfer.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int rest(String marker) throws IOException
    {
        return this.sendCommand(FTPCommand.REST, marker);
    }

    /***
     * A convenience method to send the FTP RNFR command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param pathname The pathname to rename from.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int rnfr(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.RNFR, pathname);
    }

    /***
     * A convenience method to send the FTP RNTO command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param pathname The pathname to rename to
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int rnto(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.RNTO, pathname);
    }

    /***
     * A convenience method to send the FTP DELE command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param pathname The pathname to delete.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int dele(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.DELE, pathname);
    }

    /***
     * A convenience method to send the FTP RMD command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param pathname The pathname of the directory to remove.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int rmd(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.RMD, pathname);
    }

    /***
     * A convenience method to send the FTP MKD command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param pathname The pathname of the new directory to create.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int mkd(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.MKD, pathname);
    }

    /***
     * A convenience method to send the FTP PWD command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int pwd() throws IOException
    {
        return this.sendCommand(FTPCommand.PWD);
    }

    /***
     * A convenience method to send the FTP LIST command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int list() throws IOException
    {
        return this.sendCommand(FTPCommand.LIST);
    }

    /***
     * A convenience method to send the FTP LIST command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @param pathname  The pathname to list.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int list(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.LIST, pathname);
    }

    /***
     * A convenience method to send the FTP NLST command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int nlst() throws IOException
    {
        return this.sendCommand(FTPCommand.NLST);
    }

    /***
     * A convenience method to send the FTP NLST command to the server,
     * receive the reply, and return the reply code.  Remember, it is up
     * to you to manage the data connection.  If you don't need this low
     * level of access, use <a href="org.apache.commons.net.ftp.FTPClient.html">
     * FTPClient</a>, which will handle all low level details for you.
     * <p>
     * @param pathname  The pathname to list.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int nlst(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.NLST, pathname);
    }

    /***
     * A convenience method to send the FTP SITE command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param parameters  The site parameters to send.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int site(String parameters) throws IOException
    {
        return this.sendCommand(FTPCommand.SITE, parameters);
    }

    /***
     * A convenience method to send the FTP SYST command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int syst() throws IOException
    {
        return this.sendCommand(FTPCommand.SYST);
    }

    /***
     * A convenience method to send the FTP STAT command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int stat() throws IOException
    {
        return this.sendCommand(FTPCommand.STAT);
    }

    /***
     * A convenience method to send the FTP STAT command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param pathname  A pathname to list.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int stat(String pathname) throws IOException
    {
        return this.sendCommand(FTPCommand.STAT, pathname);
    }

    /***
     * A convenience method to send the FTP HELP command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int help() throws IOException
    {
        return this.sendCommand(FTPCommand.HELP);
    }

    /***
     * A convenience method to send the FTP HELP command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @param command  The command name on which to request help.
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int help(String command) throws IOException
    {
        return this.sendCommand(FTPCommand.HELP, command);
    }

    /***
     * A convenience method to send the FTP NOOP command to the server,
     * receive the reply, and return the reply code.
     * <p>
     * @return The reply code received from the server.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending the
     *      command or receiving the server reply.
     ***/
    public int noop() throws IOException
    {
        return this.sendCommand(FTPCommand.NOOP);
    }

}
