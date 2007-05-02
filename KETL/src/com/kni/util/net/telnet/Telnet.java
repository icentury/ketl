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
import java.io.BufferedOutputStream;
import java.io.IOException;

import com.kni.util.net.SocketClient;

/**
 * @author Daniel F. Savarese
 */

class Telnet extends SocketClient
{
    static final boolean debug =  /*true;*/ false;

    static final byte[] _COMMAND_DO = {
                                          (byte)TelnetCommand.IAC, (byte)TelnetCommand.DO
                                      };

    static final byte[] _COMMAND_DONT = {
                                            (byte)TelnetCommand.IAC, (byte)TelnetCommand.DONT
                                        };

    static final byte[] _COMMAND_WILL = {
                                            (byte)TelnetCommand.IAC, (byte)TelnetCommand.WILL
                                        };

    static final byte[] _COMMAND_WONT = {
                                            (byte)TelnetCommand.IAC, (byte)TelnetCommand.WONT
                                        };

    static final byte[] _COMMAND_SB = {
                                          (byte)TelnetCommand.IAC, (byte)TelnetCommand.SB
                                      };

    static final byte[] _COMMAND_SE = {
                                          (byte)TelnetCommand.IAC, (byte)TelnetCommand.SE
                                      };

    static final int _WILL_MASK = 0x01, _DO_MASK = 0x02,
                                  _REQUESTED_WILL_MASK = 0x04, _REQUESTED_DO_MASK = 0x08;

    /* public */
    static final int DEFAULT_PORT =  23;

    int[] _doResponse, _willResponse, _options;

    /* public */
    Telnet()
    {
        this.setDefaultPort(Telnet.DEFAULT_PORT);
        this._doResponse = new int[TelnetOption.MAX_OPTION_VALUE + 1];
        this._willResponse = new int[TelnetOption.MAX_OPTION_VALUE + 1];
        this._options = new int[TelnetOption.MAX_OPTION_VALUE + 1];
    }


    boolean _stateIsWill(int option)
    {
        return ((this._options[option] & Telnet._WILL_MASK) != 0);
    }

    boolean _stateIsWont(int option)
    {
        return !this._stateIsWill(option);
    }

    boolean _stateIsDo(int option)
    {
        return ((this._options[option] & Telnet._DO_MASK) != 0);
    }

    boolean _stateIsDont(int option)
    {
        return !this._stateIsDo(option);
    }

    boolean _requestedWill(int option)
    {
        return ((this._options[option] & Telnet._REQUESTED_WILL_MASK) != 0);
    }

    boolean _requestedWont(int option)
    {
        return !this._requestedWill(option);
    }

    boolean _requestedDo(int option)
    {
        return ((this._options[option] & Telnet._REQUESTED_DO_MASK) != 0);
    }

    boolean _requestedDont(int option)
    {
        return !this._requestedDo(option);
    }

    void _setWill(int option)
    {
        this._options[option] |= Telnet._WILL_MASK;
    }
    void _setDo(int option)
    {
        this._options[option] |= Telnet._DO_MASK;
    }
    void _setWantWill(int option)
    {
        this._options[option] |= Telnet._REQUESTED_WILL_MASK;
    }
    void _setWantDo(int option)
    {
        this._options[option] |= Telnet._REQUESTED_DO_MASK;
    }

    void _setWont(int option)
    {
        this._options[option] &= ~Telnet._WILL_MASK;
    }
    void _setDont(int option)
    {
        this._options[option] &= ~Telnet._DO_MASK;
    }
    void _setWantWont(int option)
    {
        this._options[option] &= ~Telnet._REQUESTED_WILL_MASK;
    }
    void _setWantDont(int option)
    {
        this._options[option] &= ~Telnet._REQUESTED_DO_MASK;
    }

    void _processDo(int option) throws IOException
    {
        boolean acceptNewState = false;

        if (this._willResponse[option] > 0)
        {
            --this._willResponse[option];
            if (this._willResponse[option] > 0 && this._stateIsWill(option))
                --this._willResponse[option];
        }

        if (this._willResponse[option] == 0)
        {
            if (this._requestedWont(option))
            {

                switch (option)
                {

                default:
                    break;

                }


                if (acceptNewState)
                {
                    this._setWantWill(option);
                    this._sendWill(option);
                }
                else
                {
                    ++this._willResponse[option];
                    this._sendWont(option);
                }
            }
            else
            {
                // Other end has acknowledged option.

                switch (option)
                {

                default:
                    break;

                }

            }
        }

        this._setWill(option);
    }


    void _processDont(int option) throws IOException
    {
        if (this._willResponse[option] > 0)
        {
            --this._willResponse[option];
            if (this._willResponse[option] > 0 && this._stateIsWont(option))
                --this._willResponse[option];
        }

        if (this._willResponse[option] == 0 && this._requestedWill(option))
        {

            switch (option)
            {

            default:
                break;

            }

            this._setWantWont(option);

            if (this._stateIsWill(option))
                this._sendWont(option);
        }

        this._setWont(option);
    }


    void _processWill(int option) throws IOException
    {
        boolean acceptNewState = false;

        if (this._doResponse[option] > 0)
        {
            --this._doResponse[option];
            if (this._doResponse[option] > 0 && this._stateIsDo(option))
                --this._doResponse[option];
        }

        if (this._doResponse[option] == 0 && this._requestedDont(option))
        {

            switch (option)
            {

            default:
                break;

            }


            if (acceptNewState)
            {
                this._setWantDo(option);
                this._sendDo(option);
            }
            else
            {
                ++this._doResponse[option];
                this._sendDont(option);
            }
        }

        this._setDo(option);
    }


    void _processWont(int option) throws IOException
    {
        if (this._doResponse[option] > 0)
        {
            --this._doResponse[option];
            if (this._doResponse[option] > 0 && this._stateIsDont(option))
                --this._doResponse[option];
        }

        if (this._doResponse[option] == 0 && this._requestedDo(option))
        {

            switch (option)
            {

            default:
                break;

            }

            this._setWantDont(option);

            if (this._stateIsDo(option))
                this._sendDont(option);
        }

        this._setDont(option);
    }


    @Override
    protected void _connectAction_() throws IOException
    {
        super._connectAction_();
        this._input_ = new BufferedInputStream(this._input_);
        this._output_ = new BufferedOutputStream(this._output_);
    }


    final synchronized void _sendDo(int option)
    throws IOException
    {
        if (Telnet.debug)
            System.err.println("DO: " + TelnetOption.getOption(option));
        this._output_.write(Telnet._COMMAND_DO);
        this._output_.write(option);
    }

    final synchronized void _requestDo(int option)
    throws IOException
    {
        if ((this._doResponse[option] == 0 && this._stateIsDo(option)) ||
                this._requestedDo(option))
            return ;
        this._setWantDo(option);
        ++this._doResponse[option];
        this._sendDo(option);
    }

    final synchronized void _sendDont(int option)
    throws IOException
    {
        if (Telnet.debug)
            System.err.println("DONT: " + TelnetOption.getOption(option));
        this._output_.write(Telnet._COMMAND_DONT);
        this._output_.write(option);
    }

    final synchronized void _requestDont(int option)
    throws IOException
    {
        if ((this._doResponse[option] == 0 && this._stateIsDont(option)) ||
                this._requestedDont(option))
            return ;
        this._setWantDont(option);
        ++this._doResponse[option];
        this._sendDont(option);
    }


    final synchronized void _sendWill(int option)
    throws IOException
    {
        if (Telnet.debug)
            System.err.println("WILL: " + TelnetOption.getOption(option));
        this._output_.write(Telnet._COMMAND_WILL);
        this._output_.write(option);
    }

    final synchronized void _requestWill(int option)
    throws IOException
    {
        if ((this._willResponse[option] == 0 && this._stateIsWill(option)) ||
                this._requestedWill(option))
            return ;
        this._setWantWill(option);
        ++this._doResponse[option];
        this._sendWill(option);
    }

    final synchronized void _sendWont(int option)
    throws IOException
    {
        if (Telnet.debug)
            System.err.println("WONT: " + TelnetOption.getOption(option));
        this._output_.write(Telnet._COMMAND_WONT);
        this._output_.write(option);
    }

    final synchronized void _requestWont(int option)
    throws IOException
    {
        if ((this._willResponse[option] == 0 && this._stateIsWont(option)) ||
                this._requestedWont(option))
            return ;
        this._setWantWont(option);
        ++this._doResponse[option];
        this._sendWont(option);
    }

    final synchronized void _sendByte(int b)
    throws IOException
    {
        this._output_.write(b);
    }
}
