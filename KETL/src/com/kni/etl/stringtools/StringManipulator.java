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
 * Insert the type's description here. Creation date: (4/10/2002 11:59:44 AM)
 * 
 * @author: Administrator
 */
public class StringManipulator
{
    
    /** The Passed string. */
    private String PassedString;
    
    /** The Upper case passed string. */
    private String UpperCasePassedString;
    
    /** The String array. */
    private char[] StringArray;

    /**
     * StringManipulator constructor comment.
     */
    public StringManipulator()
    {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     * 
     * @param pStringToSearch java.lang.String
     * @param pVariableName java.lang.String
     * @param pVariableToValueSeperator java.lang.String
     * @param pMultipleVariableDelimiter java.lang.String
     * 
     * @return java.lang.String
     */
    public String getVariableByName(String pStringToSearch, String pVariableName, char[] pVariableToValueSeperator,
        char[] pMultipleVariableDelimiter)
    {
        return (this.getVariableByName(pStringToSearch, pVariableName, pVariableToValueSeperator,
            pMultipleVariableDelimiter, true));
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     * 
     * @param pStringToSearch java.lang.String
     * @param pVariableName java.lang.String
     * @param pVariableToValueSeperator java.lang.String
     * @param pMultipleVariableDelimiter java.lang.String
     * @param pCaseSensitive the case sensitive
     * 
     * @return java.lang.String
     */
    public String getVariableByName(String pStringToSearch, String pVariableName, char[] pVariableToValueSeperator,
        char[] pMultipleVariableDelimiter, boolean pCaseSensitive)
    {
        int varPos = -1;
        int varEndPos = -1;
        int varNamePos = -1;

        boolean varFound = false;

        if (pCaseSensitive == false)
        {
            if ((this.PassedString == null) || (this.PassedString.equals(pStringToSearch) == false))
            {
                this.PassedString = pStringToSearch;
                this.UpperCasePassedString = pStringToSearch.toUpperCase();
            }

            String searchStr = this.UpperCasePassedString;

            varNamePos = searchStr.indexOf(pVariableName);
        }

        // if variable found then get value
        if (varNamePos != -1)
        {
            int lastPos = -1;

            for (char element : pVariableToValueSeperator) {
                varPos = pStringToSearch.indexOf(element, varNamePos + pVariableName.length());

                if (varPos != -1)
                {
                    if ((lastPos == -1) || (lastPos > varPos))
                    {
                        lastPos = varPos;
                    }
                }
            }

            varPos = lastPos;

            // if variable seperator found
            if (varPos != -1)
            {
                lastPos = -1;

                for (char element : pMultipleVariableDelimiter) {
                    varEndPos = pStringToSearch.indexOf(element, varPos + 1);

                    if (varEndPos != -1)
                    {
                        if ((lastPos == -1) || (lastPos > varEndPos))
                        {
                            lastPos = varEndPos;
                        }
                    }
                }

                varEndPos = lastPos;

                if (varEndPos == -1)
                {
                    varEndPos = pStringToSearch.length();
                }

                // variable found
                varFound = true;
            }
        }

        if (varFound == true)
        {
            return pStringToSearch.substring(varPos + 1, varEndPos);
        }

        return null;
    }

    /**
     * Index of ignore case.
     * 
     * @param pSearchStr the search str
     * @param pItemToFind the item to find
     * 
     * @return the int
     */
    final private int indexOfIgnoreCase(String pSearchStr, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.searchIgnoreCase(pSearchStr.toCharArray(), 0, pSearchStr.length());
    }

    /**
     * Index of ignore case.
     * 
     * @param pSearchChar the search char
     * @param pLen the len
     * @param pItemToFind the item to find
     * 
     * @return the int
     */
    final private int indexOfIgnoreCase(char[] pSearchChar, int pLen, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.searchIgnoreCase(pSearchChar, 0, pLen);
    }

    /**
     * Index of ignore case.
     * 
     * @param pSearchChar the search char
     * @param pStartPos the start pos
     * @param pLen the len
     * @param pItemToFind the item to find
     * 
     * @return the int
     */
    final private int indexOfIgnoreCase(char[] pSearchChar, int pStartPos, int pLen, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.searchIgnoreCase(pSearchChar, pStartPos, pLen);
    }

    /**
     * Index of.
     * 
     * @param pSearchChar the search char
     * @param pLen the len
     * @param pItemToFind the item to find
     * 
     * @return the int
     */
    final private int indexOf(char[] pSearchChar, int pLen, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.search(pSearchChar, 0, pLen);
    }

    /**
     * Index of.
     * 
     * @param pSearchStr the search str
     * @param pItemToFind the item to find
     * 
     * @return the int
     */
    final private int indexOf(String pSearchStr, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.search(pSearchStr.toCharArray(), 0, pSearchStr.length());
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     * 
     * @param pStringToSearch java.lang.String
     * @param pVariableName java.lang.String
     * @param pVariableToValueSeperator java.lang.String
     * @param pMultipleVariableDelimiter java.lang.String
     * @param pCaseSensitive the case sensitive
     * 
     * @return java.lang.String
     */
    public String getVariableByName(String pStringToSearch, BoyerMooreAlgorithm pVariableName,
        String[] pVariableToValueSeperator, String[] pMultipleVariableDelimiter, boolean pCaseSensitive)
    {
        int varPos = -1;
        int varEndPos = -1;
        int varNamePos = -1;

        boolean varFound = false;

        if (pCaseSensitive == false)
        {
            varNamePos = this.indexOfIgnoreCase(pStringToSearch, pVariableName);
        }
        else
        {
            varNamePos = this.indexOf(pStringToSearch, pVariableName);
        }

        // if variable found then get value
        if (varNamePos != -1)
        {
            int lastPos = -1;

            for (String element : pVariableToValueSeperator) {
                int varLength = element.length();
                boolean res = pStringToSearch.regionMatches(varNamePos + pVariableName.getPatternLength(),
                        element, 0, varLength);

                if (res)
                {
                    varPos = varNamePos + pVariableName.getPatternLength() + varLength;

                    break;
                }
            }

            if (varPos != -1)
            {
                lastPos = -1;

                for (String element : pMultipleVariableDelimiter) {
                    varEndPos = pStringToSearch.indexOf(element, varPos);

                    if (varEndPos != -1)
                    {
                        if ((lastPos == -1) || (lastPos > varEndPos))
                        {
                            lastPos = varEndPos;
                        }
                    }
                }

                varEndPos = lastPos;

                if (varEndPos == -1)
                {
                    varEndPos = pStringToSearch.length();
                }

                // variable found
                varFound = true;
            }
        }

        if (varFound == true)
        {
            return pStringToSearch.substring(varPos, varEndPos);
        }

        return null;
    }

    /**
     * Gets the variable by name.
     * 
     * @param pStringToSearch the string to search
     * @param pStringLen the string len
     * @param pVariableName the variable name
     * @param pVariableToValueSeperator the variable to value seperator
     * @param pMultipleVariableDelimiter the multiple variable delimiter
     * @param pCaseSensitive the case sensitive
     * 
     * @return the variable by name
     */
    public String getVariableByName(char[] pStringToSearch, int pStringLen, BoyerMooreAlgorithm pVariableName,
        BoyerMooreAlgorithm[] pVariableToValueSeperator, BoyerMooreAlgorithm[] pMultipleVariableDelimiter,
        boolean pCaseSensitive)
    {
        int varPos = -1;
        int varEndPos = -1;
        int varNamePos = -1;

        boolean varFound = false;

        if (pCaseSensitive == false)
        {
            varNamePos = this.indexOfIgnoreCase(pStringToSearch, pStringLen, pVariableName);
        }
        else
        {
            varNamePos = this.indexOf(pStringToSearch, pStringLen, pVariableName);
        }

        // if variable found then get value
        if (varNamePos != -1)
        {
            int lastPos = -1;

            if (varNamePos > 0)
            {
                boolean match = false;

                for (BoyerMooreAlgorithm element : pMultipleVariableDelimiter) {
                    char ch = element.getPattern()[element.getPatternLength() -
                        1];

                    if (pStringToSearch[varNamePos - 1] == ch)
                    {
                        match = true;

                        break;
                    }
                }

                if (match == false)
                {
                    return null;
                }
            }

            for (BoyerMooreAlgorithm element : pVariableToValueSeperator) {
                int varLength = element.getPatternLength();
                boolean res;

                if ((varLength + pVariableName.getPatternLength() + varNamePos) > pStringLen)
                {
                    res = false;
                }
                else
                {
                    res = this.regionMatches(pStringToSearch, varNamePos + pVariableName.getPatternLength(),
                            element.getPattern(), 0, varLength);
                }

                if (res)
                {
                    varPos = varNamePos + pVariableName.getPatternLength() + varLength;

                    break;
                }
            }

            if (varPos != -1)
            {
                lastPos = -1;

                for (BoyerMooreAlgorithm element : pMultipleVariableDelimiter) {
                    varEndPos = this.indexOfIgnoreCase(pStringToSearch, varPos, pStringLen - varPos,
                            element);

                    if (varEndPos != -1)
                    {
                        if ((lastPos == -1) || (lastPos > varEndPos))
                        {
                            lastPos = varEndPos;
                        }
                    }
                }

                varEndPos = lastPos;

                if (varEndPos == -1)
                {
                    varEndPos = pStringLen;
                }

                // variable found
                varFound = true;
            }
        }

        if ((varFound == true) & ((varEndPos - varPos) != 0))
        {
            return String.valueOf(pStringToSearch, varPos, varEndPos - varPos);
        }

        return null;
    }

    /**
     * Region matches.
     * 
     * @param pStringA the string a
     * @param pStartPosInA the start pos in a
     * @param pStringB the string b
     * @param pStartPosInB the start pos in b
     * @param pLength the length
     * 
     * @return true, if successful
     */
    public boolean regionMatches(char[] pStringA, int pStartPosInA, char[] pStringB, int pStartPosInB, int pLength)
    {
        for (int i = 0; i < pLength; i++)
        {
            if (pStringA[i + pStartPosInA] != pStringB[i + pStartPosInB])
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     * 
     * @param pStringToSearch java.lang.String
     * @param pVariableName java.lang.String
     * @param pVariableToValueSeperator java.lang.String
     * @param pMultipleVariableDelimiter java.lang.String
     * 
     * @return java.lang.String
     */
    public String getVariableByName(String pStringToSearch, String pVariableName, String pVariableToValueSeperator,
        char[] pMultipleVariableDelimiter)
    {
        return (this.getVariableByName(pStringToSearch, pVariableName, pVariableToValueSeperator,
            pMultipleVariableDelimiter, true));
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     * 
     * @param pStringToSearch java.lang.String
     * @param pVariableName java.lang.String
     * @param pVariableToValueSeperator java.lang.String
     * @param pMultipleVariableDelimiter java.lang.String
     * @param pCaseSensitive the case sensitive
     * 
     * @return java.lang.String
     */
    public String getVariableByName(String pStringToSearch, String pVariableName, String pVariableToValueSeperator,
        char[] pMultipleVariableDelimiter, boolean pCaseSensitive)
    {
        int varPos = -1;
        int varEndPos = -1;
        int varNamePos = -1;

        boolean varFound = false;

        if (pCaseSensitive == false)
        {
            if ((this.PassedString == null) || (this.PassedString.equals(pStringToSearch) == false))
            {
                this.PassedString = pStringToSearch;
                this.UpperCasePassedString = pStringToSearch.toUpperCase();
            }

            String searchStr = this.UpperCasePassedString;

            varNamePos = searchStr.indexOf(pVariableName);
        }

        // if variable found then get value
        if (varNamePos != -1)
        {
            varPos = pStringToSearch.indexOf(pVariableToValueSeperator, varNamePos + pVariableName.length());

            // if variable seperator found
            if (varPos != -1)
            {
                int lastPos = -1;

                for (char element : pMultipleVariableDelimiter) {
                    varEndPos = pStringToSearch.indexOf(element, varPos + 1);

                    if ((lastPos == -1) || (lastPos > varEndPos))
                    {
                        lastPos = varEndPos;
                    }
                }

                varEndPos = lastPos;

                if (varEndPos == -1)
                {
                    varEndPos = pStringToSearch.length();
                }

                // variable found
                varFound = true;
            }
        }

        if (varFound == true)
        {
            return pStringToSearch.substring(varPos + pVariableToValueSeperator.length(), varEndPos);
        }

        return null;
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     * 
     * @param pStringToSearch java.lang.String
     * @param pVariableName java.lang.String
     * @param pVariableToValueSeperator java.lang.String
     * @param pMultipleVariableDelimiter java.lang.String
     * 
     * @return java.lang.String
     */
    public String getVariableByName(String pStringToSearch, String pVariableName, String pVariableToValueSeperator,
        String pMultipleVariableDelimiter)
    {
        int varPos = -1;
        int varEndPos = -1;
        int varNamePos = -1;
        String result = null;

        varNamePos = pStringToSearch.indexOf(pVariableName);

        // if variable found then get value
        if (varNamePos != -1)
        {
            varPos = pStringToSearch.indexOf(pVariableToValueSeperator, varNamePos + pVariableName.length());

            // if variable seperator found
            if (varPos != -1)
            {
                varEndPos = pStringToSearch.indexOf(pMultipleVariableDelimiter, varPos + 1);

                if (varEndPos == -1)
                {
                    varEndPos = pStringToSearch.length();
                }

                result = pStringToSearch.substring(varPos + pVariableToValueSeperator.length(), varEndPos);
            }
        }

        if ((result != null) && (result.length() > 0))
        {
            return pStringToSearch.substring(varPos + pVariableToValueSeperator.length(), varEndPos);
        }

        return null;
    }
}
