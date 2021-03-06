package com.kni.util.net.smtp;

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

/***
 * This class is used to construct a bare minimum
 * acceptable header for an email message.  To construct more
 * complicated headers you should refer to RFC 822.  When the
 * Java Mail API is finalized, you will be
 * able to use it to compose fully compliant Internet text messages.
 * <p>
 * The main purpose of the class is to faciliatate the mail sending
 * process, by relieving the programmer from having to explicitly format
 * a simple message header.  For example:
 * <pre>
 * writer = client.sendMessageData();
 * if(writer == null) // failure
 *   return false;
 * header = 
 *    new SimpleSMTPHeader("foobar@foo.com", "foo@bar.com" "Just testing"); 
 * header.addCC("bar@foo.com");
 * header.addHeaderField("Organization", "Foobar, Inc.");
 * writer.write(header.toString());
 * writer.write("This is just a test");
 * writer.close();
 * if(!client.completePendingCommand()) // failure
 *   return false;
 * </pre>
 * <p>
 * <p>
 * @author Daniel F. Savarese
 * @see SMTPClient
 ***/

public class SimpleSMTPHeader
{
    private String __subject, __from, __to;
    private StringBuffer __headerFields, __cc;

    /***
     * Creates a new SimpleSMTPHeader instance initialized with the given
     * from, to, and subject header field values.
     * <p>
     * @param from  The value of the <code>From:</code> header field.  This
     *              should be the sender's email address.
     * @param from  The value of the <code>To:</code> header field.  This
     *              should be the recipient's email address.
     * @param subject  The value of the <code>Subject:</code> header field. 
     *              This should be the subject of the message.
     ***/
    public SimpleSMTPHeader(String from, String to, String subject)
    {
        this.__to = to;
        this.__from = from;
        this.__subject = subject;
        this.__headerFields = new StringBuffer();
        this.__cc = null;
    }

    /***
     * Adds an arbitrary header field with the given value to the article
     * header.  These headers will be written before the From, To, Subject, and
     * Cc fields when the SimpleSMTPHeader is convertered to a string.
     * An example use would be:
     * <pre>
     * header.addHeaderField("Organization", "Foobar, Inc.");
     * </pre>
     * <p>
     * @param headerField  The header field to add, not including the colon.
     * @param value  The value of the added header field.
     ***/
    public void addHeaderField(String headerField, String value)
    {
        this.__headerFields.append(headerField);
        this.__headerFields.append(": ");
        this.__headerFields.append(value);
        this.__headerFields.append('\n');
    }


    /***
     * Add an email address to the CC (carbon copy or courtesy copy) list.
     * <p>
     * @param address The email address to add to the CC list.
     ***/
    public void addCC(String address)
    {
        if (this.__cc == null)
            this.__cc = new StringBuffer();
        else
            this.__cc.append(", ");

        this.__cc.append(address);
    }


    /***
     * Converts the SimpleSMTPHeader to a properly formatted header in
     * the form of a String, including the blank line used to separate
     * the header from the article body.
     * <p>
     * @return The message header in the form of a String.
     ***/
    @Override
    public String toString()
    {
        StringBuffer header = new StringBuffer();

        if (this.__headerFields.length() > 0)
            header.append(this.__headerFields.toString());

        header.append("From: ");
        header.append(this.__from);
        header.append("\nTo: ");
        header.append(this.__to);

        if (this.__cc != null)
        {
            header.append("\nCc: ");
            header.append(this.__cc);
        }

        header.append("\nSubject: ");
        header.append(this.__subject);
        header.append('\n');

        header.append('\n');

        return header.toString();
    }
}



