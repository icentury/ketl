/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.stringtools;


/**
 * Insert the type's description here.
 * Creation date: (5/17/2002 4:03:52 PM)
 * @author: Administrator
 */
public class StringMatcher implements Match
{
    /**
     * StringMatcher constructor comment.
     */

    // Any single character
    public static final char ANY = '?';

    // Zero or more characters
    public static final char MORE = '*';

    // Relevant under Windows
    public static final String DOS = "*.*";
    protected String pattern;
    protected boolean windows;

    public StringMatcher(String pattern)
    {
        this(pattern, true);
    }

    public StringMatcher(String pattern, boolean windows)
    {
        this.pattern = pattern;
        this.windows = windows;
    }

    public boolean match(String text)
    {
        return match(pattern, text);
    }

    protected boolean match(String wild, String text)
    {
        if (wild.length() == 0)
        {
            return true;
        }

        if (text.length() == 0)
        {
            return false;
        }

        if (windows && wild.equalsIgnoreCase(DOS))
        {
            return true;
        }

        int j = 0;

        for (int i = 0; i < wild.length(); i++)
        {
            if (j > text.length())
            {
                return false;
            }

            // We have a '?' wildcard, match any character
            else if (wild.charAt(i) == ANY)
            {
                j++;
            }

            // We have a '*' wildcard, check for 
            // a match in the tail
            else if (wild.charAt(i) == MORE)
            {
                for (int f = j; f < text.length(); f++)
                {
                    if (match(wild.substring(i + 1), text.substring(f)))
                    {
                        return true;
                    }
                }

                return false;
            }

            // Both characters match, case insensitive
            else if ((j < text.length()) &&
                    (Character.toUpperCase(wild.charAt(i)) != Character.toUpperCase(text.charAt(j))))
            {
                return false;
            }
            else
            {
                j++;
            }
        }

        if (j == text.length())
        {
            return true;
        }

        return false;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return this.pattern;
    }
}
