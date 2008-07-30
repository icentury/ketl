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
package com.kni.etl.stringtools;


// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here.
 * Creation date: (5/17/2002 4:03:52 PM)
 * 
 * @author: Administrator
 */
public class StringMatcher implements Match
{
    
    /** StringMatcher constructor comment. */

    // Any single character
    public static final char ANY = '?';

    // Zero or more characters
    /** The Constant MORE. */
    public static final char MORE = '*';

    // Relevant under Windows
    /** The Constant DOS. */
    public static final String DOS = "*.*";
    
    /** The pattern. */
    protected String pattern;
    
    /** The windows. */
    protected boolean windows;

    /**
     * Instantiates a new string matcher.
     * 
     * @param pattern the pattern
     */
    public StringMatcher(String pattern)
    {
        this(pattern, true);
    }

    /**
     * Instantiates a new string matcher.
     * 
     * @param pattern the pattern
     * @param windows the windows
     */
    public StringMatcher(String pattern, boolean windows)
    {
        this.pattern = pattern;
        this.windows = windows;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.stringtools.Match#match(java.lang.String)
     */
    public boolean match(String text)
    {
        return this.match(this.pattern, text);
    }

    /**
     * Match.
     * 
     * @param wild the wild
     * @param text the text
     * 
     * @return true, if successful
     */
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

        if (this.windows && wild.equalsIgnoreCase(StringMatcher.DOS))
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
            else if (wild.charAt(i) == StringMatcher.ANY)
            {
                j++;
            }

            // We have a '*' wildcard, check for 
            // a match in the tail
            else if (wild.charAt(i) == StringMatcher.MORE)
            {
                for (int f = j; f < text.length(); f++)
                {
                    if (this.match(wild.substring(i + 1), text.substring(f)))
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
    @Override
    public String toString()
    {
        return this.pattern;
    }
}
