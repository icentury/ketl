/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.stringtools;


/**
 * Insert the type's description here. Creation date: (4/10/2002 11:59:44 AM)
 *
 * @author: Administrator
 */
public class StringManipulator
{
    private String PassedString;
    private String UpperCasePassedString;
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
     * @return java.lang.String
     * @param pStringToSearch
     *            java.lang.String
     * @param pVariableName
     *            java.lang.String
     * @param pVariableToValueSeperator
     *            java.lang.String
     * @param pMultipleVariableDelimiter
     *            java.lang.String
     */
    public String getVariableByName(String pStringToSearch, String pVariableName, char[] pVariableToValueSeperator,
        char[] pMultipleVariableDelimiter)
    {
        return (getVariableByName(pStringToSearch, pVariableName, pVariableToValueSeperator,
            pMultipleVariableDelimiter, true));
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     *
     * @return java.lang.String
     * @param pStringToSearch
     *            java.lang.String
     * @param pVariableName
     *            java.lang.String
     * @param pVariableToValueSeperator
     *            java.lang.String
     * @param pMultipleVariableDelimiter
     *            java.lang.String
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

            // look for direct characters e.g '='
            for (int index = 0; index < pVariableToValueSeperator.length; index++)
            {
                varPos = pStringToSearch.indexOf(pVariableToValueSeperator[index], varNamePos + pVariableName.length());

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

                for (int index = 0; index < pMultipleVariableDelimiter.length; index++)
                {
                    varEndPos = pStringToSearch.indexOf(pMultipleVariableDelimiter[index], varPos + 1);

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

    final private int indexOfIgnoreCase(String pSearchStr, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.searchIgnoreCase(pSearchStr.toCharArray(), 0, pSearchStr.length());
    }

    final private int indexOfIgnoreCase(char[] pSearchChar, int pLen, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.searchIgnoreCase(pSearchChar, 0, pLen);
    }

    final private int indexOfIgnoreCase(char[] pSearchChar, int pStartPos, int pLen, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.searchIgnoreCase(pSearchChar, pStartPos, pLen);
    }

    final private int indexOf(char[] pSearchChar, int pLen, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.search(pSearchChar, 0, pLen);
    }

    final private int indexOf(String pSearchStr, BoyerMooreAlgorithm pItemToFind)
    {
        return pItemToFind.search(pSearchStr.toCharArray(), 0, pSearchStr.length());
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     *
     * @return java.lang.String
     * @param pStringToSearch
     *            java.lang.String
     * @param pVariableName
     *            java.lang.String
     * @param pVariableToValueSeperator
     *            java.lang.String
     * @param pMultipleVariableDelimiter
     *            java.lang.String
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
            varNamePos = indexOfIgnoreCase(pStringToSearch, pVariableName);
        }
        else
        {
            varNamePos = indexOf(pStringToSearch, pVariableName);
        }

        // if variable found then get value
        if (varNamePos != -1)
        {
            int lastPos = -1;

            // look for direct characters e.g '='
            for (int index = 0; index < pVariableToValueSeperator.length; index++)
            {
                int varLength = pVariableToValueSeperator[index].length();
                boolean res = pStringToSearch.regionMatches(varNamePos + pVariableName.getPatternLength(),
                        pVariableToValueSeperator[index], 0, varLength);

                if (res)
                {
                    varPos = varNamePos + pVariableName.getPatternLength() + varLength;

                    break;
                }
            }

            if (varPos != -1)
            {
                lastPos = -1;

                for (int index = 0; index < pMultipleVariableDelimiter.length; index++)
                {
                    varEndPos = pStringToSearch.indexOf(pMultipleVariableDelimiter[index], varPos);

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
            varNamePos = indexOfIgnoreCase(pStringToSearch, pStringLen, pVariableName);
        }
        else
        {
            varNamePos = indexOf(pStringToSearch, pStringLen, pVariableName);
        }

        // if variable found then get value
        if (varNamePos != -1)
        {
            int lastPos = -1;

            if (varNamePos > 0)
            {
                boolean match = false;

                for (int index = 0; index < pMultipleVariableDelimiter.length; index++)
                {
                    char ch = pMultipleVariableDelimiter[index].getPattern()[pMultipleVariableDelimiter[index].getPatternLength() -
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

            // look for direct characters e.g '='
            for (int index = 0; index < pVariableToValueSeperator.length; index++)
            {
                int varLength = pVariableToValueSeperator[index].getPatternLength();
                boolean res;

                if ((varLength + pVariableName.getPatternLength() + varNamePos) > pStringLen)
                {
                    res = false;
                }
                else
                {
                    res = regionMatches(pStringToSearch, varNamePos + pVariableName.getPatternLength(),
                            pVariableToValueSeperator[index].getPattern(), 0, varLength);
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

                for (int index = 0; index < pMultipleVariableDelimiter.length; index++)
                {
                    varEndPos = indexOfIgnoreCase(pStringToSearch, varPos, pStringLen - varPos,
                            pMultipleVariableDelimiter[index]);

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
     * @return java.lang.String
     * @param pStringToSearch
     *            java.lang.String
     * @param pVariableName
     *            java.lang.String
     * @param pVariableToValueSeperator
     *            java.lang.String
     * @param pMultipleVariableDelimiter
     *            java.lang.String
     */
    public String getVariableByName(String pStringToSearch, String pVariableName, String pVariableToValueSeperator,
        char[] pMultipleVariableDelimiter)
    {
        return (getVariableByName(pStringToSearch, pVariableName, pVariableToValueSeperator,
            pMultipleVariableDelimiter, true));
    }

    /**
     * Insert the method's description here. Creation date: (4/10/2002 12:01:53
     * PM)
     *
     * @return java.lang.String
     * @param pStringToSearch
     *            java.lang.String
     * @param pVariableName
     *            java.lang.String
     * @param pVariableToValueSeperator
     *            java.lang.String
     * @param pMultipleVariableDelimiter
     *            java.lang.String
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

                for (int index = 0; index < pMultipleVariableDelimiter.length; index++)
                {
                    varEndPos = pStringToSearch.indexOf(pMultipleVariableDelimiter[index], varPos + 1);

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
     * @return java.lang.String
     * @param pStringToSearch
     *            java.lang.String
     * @param pVariableName
     *            java.lang.String
     * @param pVariableToValueSeperator
     *            java.lang.String
     * @param pMultipleVariableDelimiter
     *            java.lang.String
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
