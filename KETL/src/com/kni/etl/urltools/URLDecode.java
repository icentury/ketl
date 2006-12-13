/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.urltools;


/**
 * Insert the type's description here.
 * Creation date: (5/10/2002 2:18:27 PM)
 * @author: Administrator
 */
public class URLDecode
{
    private char[] buffer;
    public int buffersize = 3000;

    /**
     * URLDecode constructor comment.
     */
    public URLDecode()
    {
        super();
    }

    public String decode(String s) throws Exception
    {
        if (buffer == null)
        {
            buffer = new char[buffersize];
        }

        int p = 0;
        int len = s.length();

        if (len >= buffersize)
        {
            len = buffersize - 1;
        }

        for (int i = 0; i < len; i++)
        {
            char c = s.charAt(i);

            switch (c)
            {
            case '+':
                buffer[p] = ' ';
                p++;

                break;

            case '%':

                try
                {
                    buffer[p] = ((char) Integer.parseInt(s.substring(i + 1, i + 3), 16));
                    p++;
                }
                catch (NumberFormatException e)
                {
                    throw new IllegalArgumentException();
                }

                i += 2;

                break;

            default:
                buffer[p] = c;
                p++;

                break;
            }
        }

        // Undo conversion to external encoding
        //String result = sb.toString();
        return (new String(buffer, 0, p));

        //byte[] inputBytes = result.getBytes("8859_1");
        //return new String(inputBytes);
    }

    public String decode(String sp, int maxLength) throws Exception
    {
        if (buffersize < maxLength)
        {
            buffer = new char[maxLength];
        }
        else
        {
            buffer = new char[buffersize];
        }

        char[] s = sp.toCharArray();

        int p = 0;
        int len = s.length;

        if (len >= buffersize)
        {
            len = buffersize - 1;
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
                buffer[p] = ' ';
                p++;

                break;

            case '%':

                try
                {
                    buffer[p] = ((char) Integer.parseInt(sp.substring(i + 1, i + 3), 16));
                    i += 2;
                }
                catch (NumberFormatException e)
                {
                    //throw new IllegalArgumentException();
                    buffer[p] = c;
                }

                p++;

                break;

            default:
                buffer[p] = c;
                p++;

                break;
            }
        }

        // Undo conversion to external encoding
        //String result = sb.toString();
        return (new String(buffer, 0, p));

        //byte[] inputBytes = result.getBytes("8859_1");
        //return new String(inputBytes);
    }
}
