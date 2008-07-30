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
package com.kni.etl.urltools;


// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here.
 * Creation date: (5/10/2002 2:18:27 PM)
 * 
 * @author: Administrator
 */
public class URLDecode
{
    
    /** The buffer. */
    private char[] buffer;
    
    /** The buffersize. */
    public int buffersize = 3000;

    /**
     * URLDecode constructor comment.
     */
    public URLDecode()
    {
        super();
    }

    /**
     * Decode.
     * 
     * @param s the s
     * 
     * @return the string
     * 
     * @throws Exception the exception
     */
    public String decode(String s) throws Exception
    {
        if (this.buffer == null)
        {
            this.buffer = new char[this.buffersize];
        }

        int p = 0;
        int len = s.length();

        if (len >= this.buffersize)
        {
            len = this.buffersize - 1;
        }

        for (int i = 0; i < len; i++)
        {
            char c = s.charAt(i);

            switch (c)
            {
            case '+':
                this.buffer[p] = ' ';
                p++;

                break;

            case '%':

                try
                {
                    this.buffer[p] = ((char) Integer.parseInt(s.substring(i + 1, i + 3), 16));
                    p++;
                }
                catch (NumberFormatException e)
                {
                    throw new IllegalArgumentException();
                }

                i += 2;

                break;

            default:
                this.buffer[p] = c;
                p++;

                break;
            }
        }

        // Undo conversion to external encoding
        //String result = sb.toString();
        return (new String(this.buffer, 0, p));

        //byte[] inputBytes = result.getBytes("8859_1");
        //return new String(inputBytes);
    }

    /**
     * Decode.
     * 
     * @param sp the sp
     * @param maxLength the max length
     * 
     * @return the string
     * 
     * @throws Exception the exception
     */
    public String decode(String sp, int maxLength) throws Exception
    {
        if (this.buffersize < maxLength)
        {
            this.buffer = new char[maxLength];
        }
        else
        {
            this.buffer = new char[this.buffersize];
        }

        char[] s = sp.toCharArray();

        int p = 0;
        int len = s.length;

        if (len >= this.buffersize)
        {
            len = this.buffersize - 1;
        }

        if (len >= maxLength)
        {
            len = maxLength - 1;
        }

        for (int i = 0; i < len; i++)
        {
            char c = s[i];

            switch (c)
            {
            case '+':
                this.buffer[p] = ' ';
                p++;

                break;

            case '%':

                try
                {
                    this.buffer[p] = ((char) Integer.parseInt(sp.substring(i + 1, i + 3), 16));
                    i += 2;
                }
                catch (NumberFormatException e)
                {
                    //throw new IllegalArgumentException();
                    this.buffer[p] = c;
                }

                p++;

                break;

            default:
                this.buffer[p] = c;
                p++;

                break;
            }
        }

        // Undo conversion to external encoding
        //String result = sb.toString();
        return (new String(this.buffer, 0, p));

        //byte[] inputBytes = result.getBytes("8859_1");
        //return new String(inputBytes);
    }
}
