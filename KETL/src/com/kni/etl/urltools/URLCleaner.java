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
 * Insert the type's description here. Creation date: (5/12/2002 12:53:41 PM)
 * 
 * @author: Administrator
 */
public class URLCleaner
{
    // Any single character
    /** The Constant ANY. */
    public static final char ANY = '?';

    // Zero or more characters
    /** The Constant MORE. */
    public static final char MORE = '*';

    // Relevant under Windows
    /** The Constant DOS. */
    public static final String DOS = "*.*";
    
    /** The param count. */
    private int paramCount = 0;
    
    /** The buffer. */
    private char[] buffer;
    
    /** The out param buffer. */
    private char[] outParamBuffer;
    
    /** The out param buffer end. */
    private int outParamBufferEnd = 0;
    
    /** The comparison buffer. */
    private char[] comparisonBuffer;
    
    /** The buffersize. */
    public int buffersize = 3000;
    
    /** The Page parser definitions. */
    private com.kni.etl.sessionizer.PageParserPageDefinition[] PageParserDefinitions;
    
    /** The protocol. */
    public String protocol = null;
    
    /** The host. */
    public String host = null;
    
    /** The port. */
    public int port;
    
    /** The file. */
    public String file = null;
    
    /** The ref. */
    public String ref = null;
    
    /** The dir start. */
    public int dirStart;
    
    /** The dir end. */
    public int dirEnd;
    
    /** The file start. */
    public int fileStart;
    
    /** The file end. */
    public int fileEnd;
    
    /** The host name start. */
    public int hostNameStart;
    
    /** The host name end. */
    public int hostNameEnd;
    
    /** The query start. */
    public int queryStart;
    
    /** The query end. */
    public int queryEnd;
    
    /** The last dir. */
    public int lastDir;
    
    /** The method start. */
    public int methodStart;
    
    /** The method end. */
    public int methodEnd;
    
    /** The protocol start. */
    public int protocolStart;
    
    /** The protocol end. */
    public int protocolEnd;
    
    /** The cleansed. */
    public boolean cleansed;
    
    /** The cleansed with ID. */
    public int cleansedWithID;
    
    /** The Parameter positions. */
    private int[] ParameterPositions = new int[256];
    
    /** The buffer end. */
    private int bufferEnd;

    /**
     * URLCleaner constructor comment.
     */
    public URLCleaner()
    {
        super();

        this.outParamBuffer = new char[this.buffersize];
    }

    /**
     * Status code match.
     * 
     * @param pErrorCode the error code
     * @param pValidCodes the valid codes
     * 
     * @return true, if successful
     */
    private final static boolean statusCodeMatch(int pErrorCode, int[] pValidCodes)
    {
        int len = pValidCodes.length;

        if (len == 0)
        {
            return true;
        }

        for (int i = 0; i < len; i++)
        {
            if (pValidCodes[i] == pErrorCode)
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 2:59:22 PM)
     * 
     * @param pHTTPRequest java.lang.String
     * @param pErrorCode the error code
     * @param start the start
     * @param maxLength the max length
     * @param decodeMIME the decode MIME
     * 
     * @return java.lang.String
     */
    public final String cleanHTTPRequest(String pHTTPRequest, int pErrorCode, int start, int maxLength,
        boolean decodeMIME)
    {
        boolean possibleMatch = false;

        // search request for details
        this.parseHTTPRequest(pHTTPRequest, start, maxLength, decodeMIME, false, true);

        // tokenise parameters
        this.tokenizeParameters();

        // Goal rebuild url according to valid Page Parser Definitions
        int len = 0;

        if (this.PageParserDefinitions != null)
        {
            len = this.PageParserDefinitions.length;
        }

        for (int i = 0; i < len; i++)
        {
            this.outParamBufferEnd = 0;

            possibleMatch = true;

            if (pErrorCode != -1)
            {
                if ((possibleMatch == true) && URLCleaner.statusCodeMatch(pErrorCode, this.PageParserDefinitions[i].getValidStatus()))
                {
                    possibleMatch = true;
                }
                else
                {
                    possibleMatch = false;
                }
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.fileStart, this.fileEnd, this.PageParserDefinitions[i].getTemplateAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.dirStart, this.dirEnd, this.PageParserDefinitions[i].getDirectoryAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.methodStart, this.methodEnd, this.PageParserDefinitions[i].getMethodAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.hostNameStart, this.hostNameEnd,
                        this.PageParserDefinitions[i].getHostNameAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.protocolStart, this.protocolEnd,
                        this.PageParserDefinitions[i].getProtocolAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if (possibleMatch == true)
            {
                this.paramCount = 0;

                // check parameters, some are compulsory some are not, if no parameters supplied then remove all
                // parameters
                if (this.PageParserDefinitions[i].getValidPageParameters() != null)
                {
                    int plen = this.PageParserDefinitions[i].getValidPageParameters().length;

                    for (int p = 0; p < plen; p++)
                    {
                        possibleMatch = this.handleParameter(this.PageParserDefinitions[i].getValidPageParameters()[p]);

                        if (possibleMatch == false)
                        {
                            p = plen;
                        }
                    }
                }
            }

            // if possible match and valid page then ok
            if (possibleMatch && this.PageParserDefinitions[i].getValidPage())
            {
                this.cleansed = possibleMatch;
            }
            else if (possibleMatch) // match but invalid page then do not cleanse
            {
                this.cleansed = false;

                return new String(this.buffer, 0, this.bufferEnd);
            }
            else
            {
                this.cleansed = false;
            }

            if (possibleMatch == true) // Overwrite url in buffer with new one
            {
                // if cleansed record which definition was used
                this.cleansedWithID = this.PageParserDefinitions[i].getID();

                if ((this.queryEnd != -1) && (this.outParamBufferEnd != 0))
                {
                    System.arraycopy(this.outParamBuffer, 0, this.buffer, this.queryStart, this.outParamBufferEnd);
                    System.arraycopy(this.buffer, this.queryEnd + 1, this.buffer, this.queryStart + this.outParamBufferEnd,
                        (this.protocolEnd - this.protocolStart) + 2);
                    this.bufferEnd = this.queryStart + this.outParamBufferEnd + (this.protocolEnd - this.protocolStart) + 2;
                }
                else if ((this.queryEnd != -1) && (this.outParamBufferEnd == 0))
                {
                    System.arraycopy(this.buffer, this.queryEnd + 1, this.buffer, this.queryStart - 1,
                        (this.protocolEnd - this.protocolStart) + 2);
                    this.bufferEnd = this.queryStart + (this.protocolEnd - this.protocolStart) + 1;
                }

                i = len;
            }

            // if still true
        }

        // private com.kni.etl.PageParserPageParameter[] ValidPageParameters;
        return (new String(this.buffer, 0, this.bufferEnd));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 2:59:22 PM)
     * 
     * @param pURLRequest the URL request
     * @param pErrorCode the error code
     * @param start the start
     * @param maxLength the max length
     * @param decodeMIME the decode MIME
     * 
     * @return java.lang.String
     */
    public final String cleanURL(String pURLRequest, int pErrorCode, int start, int maxLength, boolean decodeMIME)
    {
        boolean possibleMatch = false;

        // search request for details
        this.parseURL(pURLRequest, start, maxLength, decodeMIME, false);

        // tokenise parameters
        this.tokenizeParameters();

        // Goal rebuild url according to valid Page Parser Definitions
        int len = 0;

        if (this.PageParserDefinitions != null)
        {
            len = this.PageParserDefinitions.length;
        }

        for (int i = 0; i < len; i++)
        {
            this.outParamBufferEnd = 0;

            possibleMatch = true;

            if (pErrorCode != -1)
            {
                if ((possibleMatch == true) && URLCleaner.statusCodeMatch(pErrorCode, this.PageParserDefinitions[i].getValidStatus()))
                {
                    possibleMatch = true;
                }
                else
                {
                    possibleMatch = false;
                }
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.fileStart, this.fileEnd, this.PageParserDefinitions[i].getTemplateAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.dirStart, this.dirEnd, this.PageParserDefinitions[i].getDirectoryAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.hostNameStart, this.hostNameEnd,
                        this.PageParserDefinitions[i].getHostNameAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.protocolStart, this.protocolEnd,
                        this.PageParserDefinitions[i].getProtocolAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if (possibleMatch == true)
            {
                this.paramCount = 0;

                // check parameters, some are compulsory some are not, if no parameters supplied then remove all
                // parameters
                if (this.PageParserDefinitions[i].getValidPageParameters() != null)
                {
                    int plen = this.PageParserDefinitions[i].getValidPageParameters().length;

                    for (int p = 0; p < plen; p++)
                    {
                        possibleMatch = this.handleParameter(this.PageParserDefinitions[i].getValidPageParameters()[p]);

                        if (possibleMatch == false)
                        {
                            p = plen;
                        }
                    }
                }
            }

            // if possible match and valid page then ok
            if (possibleMatch && this.PageParserDefinitions[i].getValidPage())
            {
                this.cleansed = possibleMatch;
            }
            else if (possibleMatch) // match but invalid page then do not cleanse
            {
                this.cleansed = false;

                return new String(this.buffer, 0, this.bufferEnd);
            }
            else
            {
                this.cleansed = false;
            }

            if (possibleMatch == true) // Overwrite url in buffer with new one
            {
                // if cleansed record which definition was used
                this.cleansedWithID = this.PageParserDefinitions[i].getID();

                if ((this.queryEnd != -1) && (this.outParamBufferEnd != 0))
                {
                    System.arraycopy(this.outParamBuffer, 0, this.buffer, this.queryStart, this.outParamBufferEnd);

                    // System.arraycopy(this.buffer, this.queryEnd + 1, this.buffer, queryStart +
                    // this.outParamBufferEnd, (this.protocolEnd - this.protocolStart) + 2);
                    this.bufferEnd = this.queryStart + this.outParamBufferEnd;
                }
                else if ((this.queryEnd != -1) && (this.outParamBufferEnd == 0))
                {
                    this.bufferEnd = this.queryStart;
                }

                i = len;
            }

            // if still true
        }

        // private com.kni.etl.PageParserPageParameter[] ValidPageParameters;
        return (new String(this.buffer, 0, this.bufferEnd));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:55:11 PM)
     * 
     * @return com.kni.etl.PageParserPageDefinition[]
     */
    public final com.kni.etl.sessionizer.PageParserPageDefinition[] getPageParserDefinitions()
    {
        return this.PageParserDefinitions;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 5:37:43 PM)
     * 
     * @param pParameter the parameter
     * 
     * @return true, if handle parameter
     */
    private final boolean handleParameter(com.kni.etl.sessionizer.PageParserPageParameter pParameter)
    {
        if (pParameter == null)
        {
            return (true);
        }

        int paramStart = this.queryStart;
        int valPos = -1;

        for (int i = 0; i < 256; i++)
        {
            if (this.ParameterPositions[i] == -1)
            {
                i = 256;
            }
            else
            {
                valPos = this.parameterInBuffer(paramStart, this.ParameterPositions[i], pParameter.getParameterName(),
                        pParameter.getValueSeperator());

                if (valPos == 0)
                {
                    valPos = this.ParameterPositions[i] - 1;
                }

                if ((valPos != -1) && (pParameter.isRemoveParameter() == false))
                { // write param name to

                    // outParamBuffer
                    this.paramCount++;

                    if (this.paramCount > 1)
                    {
                        this.outParamBuffer[this.outParamBufferEnd] = '&';
                        this.outParamBufferEnd++;
                    }

                    System.arraycopy(this.buffer, paramStart, this.outParamBuffer, this.outParamBufferEnd,
                        (valPos - paramStart));
                    this.outParamBufferEnd = (this.outParamBufferEnd + (valPos - paramStart)) - 1;
                }

                if ((valPos != -1) && (pParameter.isRemoveParameter() == false) &&
                        (pParameter.isRemoveParameterValue() == false) && (valPos <= (this.ParameterPositions[i] - 1)))
                { // write
                  // param
                  // value
                  // to
                  // outParamBuffer
                    System.arraycopy(this.buffer, valPos, this.outParamBuffer, this.outParamBufferEnd + 1,
                        (this.ParameterPositions[i] - valPos) + 1);
                    this.outParamBufferEnd = this.outParamBufferEnd + (this.ParameterPositions[i] - valPos) + 2;
                }
                else if ((valPos != -1) && (pParameter.isRemoveParameter() == false) &&
                        (pParameter.isRemoveParameterValue() == false) && (valPos >= (this.ParameterPositions[i] - 1)))
                {
                    // use to be + 2 mayhave got the maths wrong
                    this.outParamBufferEnd = this.outParamBufferEnd + 1;
                }

                paramStart = this.ParameterPositions[i] + 2;

                if (valPos != -1)
                {
                    return (true);
                }
            }
        }

        if ((pParameter.isParameterRequired() == true) && (valPos == -1))
        {
            return (false);
        }

        return (true);
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 1:12:03 PM)
     * 
     * @param pChar char
     * @param pArray the array
     * @param pStartPos the start pos
     * 
     * @return int
     */
    public static final int indexOfCharInArray(char[] pArray, char pChar, int pStartPos)
    {
        int len = pArray.length;

        for (int i = pStartPos; i < len; i++)
        {
            if (pArray[i] == pChar)
            {
                return (i);
            }
        }

        return -1;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 1:12:03 PM)
     * 
     * @param pChar char
     * 
     * @return int
     */
    public static final int indexOfCharInBuffer(char pChar)
    {
        return 0;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 3:24:44 PM)
     * 
     * @param pData the data
     * @param pBufferStartPos the buffer start pos
     * @param pBufferEndPos the buffer end pos
     * @param pSearchString the search string
     * 
     * @return int
     */
    public static final boolean wildcardMatch(char[] pData, int pBufferStartPos, int pBufferEndPos, char[] pSearchString)
    {
        // wildcardMatch( String search, String data ) {
        if ((pData == null) || (pSearchString == null))
        {
            return (false);
        }

        char[] w = pSearchString;
        char[] d = pData;
        int ch;
        int prevCh = 0;
        int starMi = -1;
        int starMj = -1;
        int i = 0;
        int j = pBufferStartPos;

        int dlength = pBufferEndPos + 1;
        int wlength = w.length;

        while (j < dlength)
        {
            while (i < wlength)
            {
                ch = w[i];

                if (ch == URLCleaner.MORE)
                {
                    j--; // '*' may match a zero length substring
                }
                else if (ch == URLCleaner.ANY)
                {
                    if (prevCh == URLCleaner.MORE)
                    {
                        ch = URLCleaner.MORE;
                    }
                }
                else if (prevCh == URLCleaner.MORE)
                {
                    j--;

                    while (++j < dlength)
                    {
                        if (ch == d[j])
                        {
                            break;
                        }
                    }

                    starMi = i;
                    starMj = j;
                    starMj++;
                }
                else if (j < dlength)
                {
                    if (ch != d[j])
                    {
                        if (starMi >= 0)
                        {
                            i = starMi;
                            j = starMj;
                            prevCh = URLCleaner.MORE;

                            continue;
                        }

                        return (false);
                    }
                }
                else
                {
                    return (false);
                }

                i++;
                j++;
                prevCh = ch;
            }

            if (j < dlength)
            {
                if (prevCh == URLCleaner.MORE)
                {
                    return (true);
                }
                else if (starMi < 0)
                {
                    return (false);
                }
                else
                {
                    i = starMi;
                    j = starMj;
                    prevCh = URLCleaner.MORE;
                }
            }
        }

        if (j == dlength)
        {
            return (true);
        }

        return (false);
    }

    /**
     * DOCUMENT ME!.
     * 
     * @param pBufferStartPos DOCUMENT ME!
     * @param pBufferEndPos DOCUMENT ME!
     * @param pCharArrayToCompare DOCUMENT ME!
     * 
     * @return DOCUMENT ME!
     */
    public final int likeBuffer(int pBufferStartPos, int pBufferEndPos, char[] pCharArrayToCompare)
    {
        // if both are empty then return true
        if ((pCharArrayToCompare != null) && (pCharArrayToCompare[0] == '*') && (pCharArrayToCompare.length == 1))
        {
            return (0);
        }

        if ((pBufferStartPos == -1) && (pCharArrayToCompare == null))
        {
            return (0);
        }

        if ((pCharArrayToCompare == null) && ((pBufferEndPos - pBufferStartPos) == 0))
        {
            return (0);
        }

        if (pCharArrayToCompare == null)
        {
            return (-1);
        }

        if (pBufferStartPos == -1)
        {
            return (-1);
        }

        // this.comparisonBuffer = pCharArrayToCompare;
        if (URLCleaner.wildcardMatch(this.buffer, pBufferStartPos, pBufferEndPos, pCharArrayToCompare))
        {
            return 0;
        }

        return -1;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 3:24:44 PM)
     * 
     * @param pBufferStartPos the buffer start pos
     * @param pBufferEndPos the buffer end pos
     * @param pCharArrayToCompare the char array to compare
     * 
     * @return int
     */
    public final int oldlikeBuffer(int pBufferStartPos, int pBufferEndPos, char[] pCharArrayToCompare)
    {
        // if both are empty then return true
        if ((pCharArrayToCompare != null) && (pCharArrayToCompare[0] == '*') && (pCharArrayToCompare.length == 1))
        {
            return (0);
        }

        if ((pBufferStartPos == -1) && (pCharArrayToCompare == null))
        {
            return (0);
        }

        if ((pCharArrayToCompare == null) && ((pBufferEndPos - pBufferStartPos) == 0))
        {
            return (0);
        }

        if (pCharArrayToCompare == null)
        {
            return (-1);
        }

        if (pBufferStartPos == -1)
        {
            return (-1);
        }

        this.comparisonBuffer = pCharArrayToCompare;

        /*
         * String str = new String(buffer); if (str.indexOf("rd.jhtml")>1) { int r = 2; }
         */
        return (this.likeBuffer(pBufferStartPos, pBufferEndPos, 0, pCharArrayToCompare.length - 1));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 3:24:44 PM)
     * 
     * @param pBufferStartPos the buffer start pos
     * @param pBufferEndPos the buffer end pos
     * @param pCompStartPos the comp start pos
     * @param pCompEndPos the comp end pos
     * 
     * @return int
     */
    public final int likeBuffer(int pBufferStartPos, int pBufferEndPos, int pCompStartPos, int pCompEndPos)
    {
        if (((pCompEndPos - pCompStartPos) == 0) && ((pBufferEndPos - pBufferStartPos) == 0))
        {
            if ((this.comparisonBuffer[pCompStartPos] == URLCleaner.ANY) || (this.comparisonBuffer[pCompStartPos] == URLCleaner.MORE))
            {
                return 0;
            }

            if (this.comparisonBuffer[pCompStartPos] == this.buffer[pBufferStartPos])
            {
                return 0;
            }
        }

        if ((pBufferEndPos - pBufferStartPos) == 0)
        {
            return -1;
        }

        if ((pCompEndPos - pCompStartPos) == 0)
        {
            return -1;
        }

        int j = pBufferStartPos;

        for (int i = pCompStartPos; i < (pCompEndPos + 1); i++)
        {
            if (j > (pBufferEndPos + 1))
            {
                return -1;
            }

            // We have a '?' wildcard, match any character
            else if (this.comparisonBuffer[i] == URLCleaner.ANY)
            {
                j++;
            }

            // We have a '*' wildcard, check for
            // a match in the tail
            else if (this.comparisonBuffer[i] == URLCleaner.MORE)
            {
                for (int f = j; f < (pBufferEndPos + 1); f++)
                {
                    if (this.likeBuffer(f, pBufferEndPos, i + 1, pCompEndPos) == 0)
                    {
                        return 0;
                    }
                }

                return -1;
            }

            // Both characters match, case insensitive
            else if ((j < (pBufferEndPos + 1)) && (this.comparisonBuffer[i] != this.buffer[j]))
            {
                return -1;
            }
            else
            {
                j++;
            }
        }

        if (j == (pBufferEndPos + 1))
        {
            return 0;
        }

        return -1;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 6:09:52 PM)
     * 
     * @param pStartPos the start pos
     * @param pEndPos the end pos
     * @param pParamName the param name
     * @param pValueSeperator the value seperator
     * 
     * @return int
     */
    public final int parameterInBuffer(int pStartPos, int pEndPos, char[] pParamName, char[] pValueSeperator)
    {
        int pos = 0;
        int sepLen = 0;

        if (pStartPos >= pEndPos)
        {
            return (-1);
        }

        if (pValueSeperator != null)
        {
            sepLen = pValueSeperator.length;
        }

        int pLen = pParamName.length - 1;

        for (int i = pStartPos; i <= pEndPos; i++)
        {
            if ((pos <= pLen) && (pParamName[pos] != this.buffer[i]))
            {
                return (-1);
            }
            else if ((pos > pLen) && (pos <= (pLen + sepLen)) && (pValueSeperator[pos - (pLen + 1)] != this.buffer[i]))
            {
                return (-1);
            }
            else if (pos >= (pLen + sepLen))
            {
                return (i);
            }

            pos++;
        }

        return (0);
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:56:12 PM)
     * 
     * @param pHTTPRequest the HTTP request
     * @param start the start
     * @param limit the limit
     * 
     * @return java.lang.String
     */
    public final String parseHTTPRequest(String pHTTPRequest, int start, int limit)
    {
        return (this.parseHTTPRequest(pHTTPRequest, start, limit, true, true, true));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:56:12 PM)
     * 
     * @param pRemoveNewLines TODO
     * @param pHTTPRequest the HTTP request
     * @param start the start
     * @param maxLength the max length
     * @param decodeMIME the decode MIME
     * @param pReturnString the return string
     * 
     * @return java.lang.String
     */
    public final String parseHTTPRequest(String pHTTPRequest, int start, int maxLength, boolean decodeMIME,
        boolean pReturnString, boolean pRemoveNewLines)
    {
        this.dirStart = -1;
        this.dirEnd = -1;
        this.fileStart = -1;
        this.fileEnd = -1;
        this.queryStart = -1;
        this.queryEnd = -1;
        this.lastDir = -1;
        this.protocolStart = -1;
        this.protocolEnd = -1;
        this.hostNameStart = -1;
        this.hostNameEnd = -1;
        this.methodEnd = -1;
        this.methodStart = 0;

        int pos;
        boolean scanAgain = false;

        this.protocol = null;
        this.host = null;
        this.port = -1;
        this.file = null;
        this.ref = null;

        if (this.buffer == null)
        {
            if (this.buffersize < maxLength)
            {
                this.buffer = new char[maxLength];
            }
            else
            {
                this.buffer = new char[this.buffersize];
            }
        }
        else if (this.buffersize < maxLength)
        {
            this.buffersize = maxLength;
            this.buffer = new char[this.buffersize];
        }

        char[] s = pHTTPRequest.toCharArray();

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

        // Goals
        // find filename
        // find directory
        // find query parameters
        for (int i = start; i < len; i++)
        {
            if (decodeMIME == true)
            {
                char c = s[i];

                switch (c)
                {
                case '+':
                    this.buffer[p] = '_';
                    p++;

                    break;

                case '%':

                    try
                    {
                        int ch = Integer.parseInt(pHTTPRequest.substring(i + 1, i + 3), 16);

                        char newc = ((char) ch);
                        
                        if(pRemoveNewLines && (newc == '\n' || newc == '\r')) {
                            newc = ' ';
                        }
                        
                        this.buffer[p] = newc;

                        // if hex creates whitespace create an underscore instead
                        if (ch < 33)
                        {
                            this.buffer[p] = '_';
                        }

                        i += 2;

                        if (this.buffer[p] == ';')
                        {
                            this.buffer[p] = '?';
                        }

                        if ((this.buffer[p] == '%') && (scanAgain == false))
                        {
                            scanAgain = true;
                        }
                    }
                    catch (NumberFormatException e)
                    {
                        // throw new IllegalArgumentException();
                        this.buffer[p] = c;
                    }
                    catch (StringIndexOutOfBoundsException e)
                    {
                        // throw new IllegalArgumentException();
                        this.buffer[p] = c;
                    }

                    p++;

                    break;

                // change all ? into semicolons
                case ';':
                    this.buffer[p] = '?';
                    p++;

                    break;

                default:
                    this.buffer[p] = c;
                    p++;

                    break;
                }
            }
            else
            {
                // just duplicate
                this.buffer[i] = s[i];
                p = i;
            }

            pos = p - 1;

            // if slash is found then directory has been found
            if (this.buffer[pos] == '/')
            {
               
                this.lastDir = pos;

                // if dirStart = -1 then first slash is directory start
                if (this.dirStart == -1)
                {
                    this.dirStart = pos;
                    
                    if (pos > 0)
                    {
                        if (this.buffer[pos - 1] == ' ')
                        {
                            this.methodEnd = pos - 2;
                        }
                        else
                        {
                            this.methodEnd = pos - 1;
                        }
                    }

                }

                // if ';' has been found query parameters have been found
            }
            else if ((this.fileEnd == -1) && ((this.buffer[pos] == ';') || (this.buffer[pos] == '?')))
            {
                this.dirEnd = this.lastDir;
                this.fileEnd = pos - 1;
                this.fileStart = this.lastDir + 1;

                if (this.fileEnd < this.fileStart)
                {
                    this.fileEnd = this.fileStart;
                }

                this.queryStart = pos + 1;

                // if ' ' has been found then url end has been found
            }
            else if ((this.buffer[pos] == ' ') && (this.lastDir != -1))
            {
                if (this.dirEnd == -1)
                {
                    this.dirEnd = this.lastDir;
                }

                if (this.fileEnd == -1)
                {
                    this.fileEnd = pos - 1;
                    this.fileStart = this.lastDir + 1;

                    if (this.fileEnd < this.fileStart)
                    {
                        this.fileEnd = this.fileStart;
                    }
                }

                if (this.queryStart != -1)
                {
                    this.queryEnd = pos - 1;
                }

                this.protocolStart = pos + 1;
            }
        }

        this.bufferEnd = p;

        if (this.protocolStart != -1)
        {
            this.protocolEnd = p - 1;
        }

        // call procedure again if hex has been hexed.
        if (scanAgain == true)
        {
            this.parseHTTPRequest(new String(this.buffer, 0, this.bufferEnd), start, maxLength, decodeMIME, false, pRemoveNewLines);
        }

        if (pReturnString == true)
        {
            return (new String(this.buffer, 0, this.bufferEnd));
        }

        return null;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:56:12 PM)
     * 
     * @param pURL the URL
     * @param start the start
     * @param limit the limit
     * 
     * @return java.lang.String
     */
    public final String parseURL(String pURL, int start, int limit)
    {
        return (this.parseURL(pURL, start, limit, true, true));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:56:12 PM)
     * 
     * @param pURL the URL
     * @param start the start
     * @param maxLength the max length
     * @param decodeMIME the decode MIME
     * @param pReturnString the return string
     * 
     * @return java.lang.String
     */
    public final String parseURL(String pURL, int start, int maxLength, boolean decodeMIME, boolean pReturnString)
    {
        this.dirStart = -1;
        this.dirEnd = -1;
        this.fileStart = -1;
        this.fileEnd = -1;
        this.queryStart = -1;
        this.queryEnd = -1;
        this.lastDir = -1;
        this.protocolStart = -1;
        this.protocolEnd = -1;
        this.hostNameStart = -1;
        this.hostNameEnd = -1;
        this.methodEnd = -1;
        this.methodStart = -1;

        int pos;

        this.protocol = null;
        this.host = null;
        this.port = -1;
        this.file = null;
        this.ref = null;

        if (this.buffer == null)
        {
            if (this.buffersize < maxLength)
            {
                this.buffer = new char[maxLength];
            }
            else
            {
                this.buffer = new char[this.buffersize];
            }
        }
        else if (this.buffersize < maxLength)
        {
            this.buffersize = maxLength;
            this.buffer = new char[this.buffersize];
        }

        char[] s = pURL.toCharArray();

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

        // Goals
        // find filename
        // find directory
        // find query parameters
        for (int i = start; i < len; i++)
        {
            if (decodeMIME == true)
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
                        // if % but pos of %+3 greater than length then ignore else parse
                        if ((i + 3) <= len)
                        {
                            this.buffer[p] = ((char) Integer.parseInt(pURL.substring(i + 1, i + 3), 16));
                            i += 2;

                            if (this.buffer[p] == ';')
                            {
                                this.buffer[p] = '?';
                            }
                        }
                    }
                    catch (NumberFormatException e)
                    {
                        // throw new IllegalArgumentException();
                        this.buffer[p] = c;
                    }

                    p++;

                    break;

                // change all ? into semicolons
                case ';':
                    this.buffer[p] = '?';
                    p++;

                    break;

                default:
                    this.buffer[p] = c;
                    p++;

                    break;
                }
            }
            else
            {
                // just duplicate
                this.buffer[i] = s[i];
                p = i;
            }

            pos = p - 1;

            // if slash is found then directory has been found
            if ((this.protocolStart == -1) && (this.buffer[pos] == 'h') &&
                    ((this.buffer[pos + 1] == 't') & (this.buffer[pos + 2] == 't')))
            {
                this.protocolStart = pos;
            }
            else if ((this.protocolStart != -1) && (this.buffer[pos] == '/') && (this.buffer[pos - 1] == '/'))
            {
                this.hostNameStart = pos + 1;

                if (this.protocolStart != -1)
                {
                    this.protocolEnd = pos - 1;
                }

                // if ';' has been found query parameters have been found
            }
            else if ((this.hostNameStart != -1) && (this.buffer[pos] == '/'))
            {
                if (this.hostNameEnd == -1)
                {
                    this.hostNameEnd = pos - 1;
                }

                this.lastDir = pos;

                // if dirStart = -1 then first slash is directory start
                if (this.dirStart == -1)
                {
                    this.dirStart = pos;
                }

                // if ';' has been found query parameters have been found
            }
            else if ((this.fileEnd == -1) && ((this.buffer[pos] == ';') || (this.buffer[pos] == '?')))
            {
                this.dirEnd = this.lastDir;

                this.fileEnd = pos - 1;

                if (this.fileEnd < this.fileStart)
                {
                    this.fileEnd = this.fileStart;
                }

                this.fileStart = this.lastDir + 1;
                this.queryStart = pos + 1;

                // if ' ' has been found then url end has been found
            }
        }

        this.bufferEnd = p;

        if (this.dirEnd == -1)
        {
            this.dirEnd = this.lastDir;
        }

        if (this.fileEnd == -1)
        {
            this.fileEnd = len - 1;
            this.fileStart = this.lastDir + 1;

            if (this.fileEnd < this.fileStart)
            {
                this.fileEnd = this.fileStart;
            }
        }

        if (this.queryStart != -1)
        {
            this.queryEnd = len - 1;
        }

        if (this.protocolEnd == -1)
        {
            this.protocolEnd = this.protocolStart + 4;
        }

        if (pReturnString == true)
        {
            return (new String(this.buffer, 0, this.bufferEnd));
        }

        return null;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:55:11 PM)
     * 
     * @param newPageParserDefinitions com.kni.etl.PageParserPageDefinition[]
     */
    public final void setPageParserDefinitions(
        com.kni.etl.sessionizer.PageParserPageDefinition[] newPageParserDefinitions)
    {
        this.PageParserDefinitions = newPageParserDefinitions;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 5:41:07 PM) Builds an index on the parameters in
     * the query seperator by ? and &
     */
    private final void tokenizeParameters()
    {
        int PPos = 0;

        if (this.queryEnd == -1)
        {
            this.ParameterPositions[PPos] = -1;

            return;
        }

        // otherwise set 0 to default to query end;
        this.ParameterPositions[PPos] = this.queryEnd;
        this.ParameterPositions[PPos + 1] = -1;

        // String str = new String(buffer,queryStart,queryEnd);
        for (int i = this.queryStart; i <= this.queryEnd; i++)
        {
            switch (this.buffer[i])
            {
            case '&':
            case '?':
                this.ParameterPositions[PPos] = i - 1;
                PPos++;

                break;
            }
        }

        if (PPos > 0)
        {
            this.ParameterPositions[PPos] = this.queryEnd;
            this.ParameterPositions[PPos + 1] = -1;
        }
    }
}
