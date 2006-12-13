/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.urltools;

/**
 * Insert the type's description here. Creation date: (5/12/2002 12:53:41 PM)
 *
 * @author: Administrator
 */
public class URLCleaner
{
    // Any single character
    public static final char ANY = '?';

    // Zero or more characters
    public static final char MORE = '*';

    // Relevant under Windows
    public static final String DOS = "*.*";
    private int paramCount = 0;
    private char[] buffer;
    private char[] outParamBuffer;
    private int outParamBufferEnd = 0;
    private char[] comparisonBuffer;
    public int buffersize = 3000;
    private com.kni.etl.sessionizer.PageParserPageDefinition[] PageParserDefinitions;
    public String protocol = null;
    public String host = null;
    public int port;
    public String file = null;
    public String ref = null;
    public int dirStart;
    public int dirEnd;
    public int fileStart;
    public int fileEnd;
    public int hostNameStart;
    public int hostNameEnd;
    public int queryStart;
    public int queryEnd;
    public int lastDir;
    public int methodStart;
    public int methodEnd;
    public int protocolStart;
    public int protocolEnd;
    public boolean cleansed;
    public int cleansedWithID;
    private int[] ParameterPositions = new int[256];
    private int bufferEnd;

    /**
     * URLCleaner constructor comment.
     */
    public URLCleaner()
    {
        super();

        this.outParamBuffer = new char[this.buffersize];
    }

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

        if (PageParserDefinitions != null)
        {
            len = PageParserDefinitions.length;
        }

        for (int i = 0; i < len; i++)
        {
            this.outParamBufferEnd = 0;

            possibleMatch = true;

            if (pErrorCode != -1)
            {
                if ((possibleMatch == true) && statusCodeMatch(pErrorCode, PageParserDefinitions[i].getValidStatus()))
                {
                    possibleMatch = true;
                }
                else
                {
                    possibleMatch = false;
                }
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.fileStart, this.fileEnd, PageParserDefinitions[i].getTemplateAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.dirStart, this.dirEnd, PageParserDefinitions[i].getDirectoryAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.methodStart, this.methodEnd, PageParserDefinitions[i].getMethodAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.hostNameStart, this.hostNameEnd,
                        PageParserDefinitions[i].getHostNameAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.protocolStart, this.protocolEnd,
                        PageParserDefinitions[i].getProtocolAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if (possibleMatch == true)
            {
                paramCount = 0;

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
            if (possibleMatch && PageParserDefinitions[i].getValidPage())
            {
                this.cleansed = possibleMatch;
            }
            else if (possibleMatch) // match but invalid page then do not cleanse
            {
                this.cleansed = false;

                return new String(buffer, 0, this.bufferEnd);
            }
            else
            {
                this.cleansed = false;
            }

            if (possibleMatch == true) // Overwrite url in buffer with new one
            {
                // if cleansed record which definition was used
                this.cleansedWithID = this.PageParserDefinitions[i].getID();

                if ((queryEnd != -1) && (this.outParamBufferEnd != 0))
                {
                    System.arraycopy(this.outParamBuffer, 0, this.buffer, this.queryStart, this.outParamBufferEnd);
                    System.arraycopy(this.buffer, this.queryEnd + 1, this.buffer, queryStart + this.outParamBufferEnd,
                        (this.protocolEnd - this.protocolStart) + 2);
                    bufferEnd = queryStart + this.outParamBufferEnd + (this.protocolEnd - this.protocolStart) + 2;
                }
                else if ((queryEnd != -1) && (this.outParamBufferEnd == 0))
                {
                    System.arraycopy(this.buffer, this.queryEnd + 1, this.buffer, queryStart - 1,
                        (this.protocolEnd - this.protocolStart) + 2);
                    bufferEnd = queryStart + (this.protocolEnd - this.protocolStart) + 1;
                }

                i = len;
            }

            // if still true
        }

        // private com.kni.etl.PageParserPageParameter[] ValidPageParameters;
        return (new String(buffer, 0, this.bufferEnd));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 2:59:22 PM)
     *
     * @param pHTTPRequest java.lang.String
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

        if (PageParserDefinitions != null)
        {
            len = PageParserDefinitions.length;
        }

        for (int i = 0; i < len; i++)
        {
            this.outParamBufferEnd = 0;

            possibleMatch = true;

            if (pErrorCode != -1)
            {
                if ((possibleMatch == true) && statusCodeMatch(pErrorCode, PageParserDefinitions[i].getValidStatus()))
                {
                    possibleMatch = true;
                }
                else
                {
                    possibleMatch = false;
                }
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.fileStart, this.fileEnd, PageParserDefinitions[i].getTemplateAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.dirStart, this.dirEnd, PageParserDefinitions[i].getDirectoryAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.hostNameStart, this.hostNameEnd,
                        PageParserDefinitions[i].getHostNameAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if ((possibleMatch == true) &&
                    (this.likeBuffer(this.protocolStart, this.protocolEnd,
                        PageParserDefinitions[i].getProtocolAsCharArray()) == 0))
            {
                possibleMatch = true;
            }
            else
            {
                possibleMatch = false;
            }

            if (possibleMatch == true)
            {
                paramCount = 0;

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
            if (possibleMatch && PageParserDefinitions[i].getValidPage())
            {
                this.cleansed = possibleMatch;
            }
            else if (possibleMatch) // match but invalid page then do not cleanse
            {
                this.cleansed = false;

                return new String(buffer, 0, this.bufferEnd);
            }
            else
            {
                this.cleansed = false;
            }

            if (possibleMatch == true) // Overwrite url in buffer with new one
            {
                // if cleansed record which definition was used
                this.cleansedWithID = this.PageParserDefinitions[i].getID();

                if ((queryEnd != -1) && (this.outParamBufferEnd != 0))
                {
                    System.arraycopy(this.outParamBuffer, 0, this.buffer, this.queryStart, this.outParamBufferEnd);

                    // System.arraycopy(this.buffer, this.queryEnd + 1, this.buffer, queryStart +
                    // this.outParamBufferEnd, (this.protocolEnd - this.protocolStart) + 2);
                    bufferEnd = queryStart + this.outParamBufferEnd;
                }
                else if ((queryEnd != -1) && (this.outParamBufferEnd == 0))
                {
                    bufferEnd = queryStart;
                }

                i = len;
            }

            // if still true
        }

        // private com.kni.etl.PageParserPageParameter[] ValidPageParameters;
        return (new String(buffer, 0, this.bufferEnd));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:55:11 PM)
     *
     * @return com.kni.etl.PageParserPageDefinition[]
     */
    public final com.kni.etl.sessionizer.PageParserPageDefinition[] getPageParserDefinitions()
    {
        return PageParserDefinitions;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 5:37:43 PM)
     *
     * @param param com.kni.etl.PageParserPageParameter
     */
    private final boolean handleParameter(com.kni.etl.sessionizer.PageParserPageParameter pParameter)
    {
        if (pParameter == null)
        {
            return (true);
        }

        int paramStart = queryStart;
        int valPos = -1;

        for (int i = 0; i < 256; i++)
        {
            if (ParameterPositions[i] == -1)
            {
                i = 256;
            }
            else
            {
                valPos = this.parameterInBuffer(paramStart, ParameterPositions[i], pParameter.getParameterName(),
                        pParameter.getValueSeperator());

                if (valPos == 0)
                {
                    valPos = ParameterPositions[i] - 1;
                }

                if ((valPos != -1) && (pParameter.isRemoveParameter() == false))
                { // write param name to

                    // outParamBuffer
                    paramCount++;

                    if (paramCount > 1)
                    {
                        this.outParamBuffer[this.outParamBufferEnd] = '&';
                        this.outParamBufferEnd++;
                    }

                    System.arraycopy(buffer, paramStart, this.outParamBuffer, this.outParamBufferEnd,
                        (valPos - paramStart));
                    this.outParamBufferEnd = (this.outParamBufferEnd + (valPos - paramStart)) - 1;
                }

                if ((valPos != -1) && (pParameter.isRemoveParameter() == false) &&
                        (pParameter.isRemoveParameterValue() == false) && (valPos <= (ParameterPositions[i] - 1)))
                { // write
                  // param
                  // value
                  // to
                  // outParamBuffer
                    System.arraycopy(buffer, valPos, this.outParamBuffer, this.outParamBufferEnd + 1,
                        (ParameterPositions[i] - valPos) + 1);
                    this.outParamBufferEnd = this.outParamBufferEnd + (ParameterPositions[i] - valPos) + 2;
                }
                else if ((valPos != -1) && (pParameter.isRemoveParameter() == false) &&
                        (pParameter.isRemoveParameterValue() == false) && (valPos >= (ParameterPositions[i] - 1)))
                {
                    // use to be + 2 mayhave got the maths wrong
                    this.outParamBufferEnd = this.outParamBufferEnd + 1;
                }

                paramStart = ParameterPositions[i] + 2;

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
     * @param pString java.lang.String
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

                if (ch == MORE)
                {
                    j--; // '*' may match a zero length substring
                }
                else if (ch == ANY)
                {
                    if (prevCh == MORE)
                    {
                        ch = MORE;
                    }
                }
                else if (prevCh == MORE)
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
                            prevCh = MORE;

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
                if (prevCh == MORE)
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
                    prevCh = MORE;
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
     * DOCUMENT ME!
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
        if (wildcardMatch(buffer, pBufferStartPos, pBufferEndPos, pCharArrayToCompare))
        {
            return 0;
        }

        return -1;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 3:24:44 PM)
     *
     * @param pString java.lang.String
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
        return (likeBuffer(pBufferStartPos, pBufferEndPos, 0, pCharArrayToCompare.length - 1));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 3:24:44 PM)
     *
     * @param pString java.lang.String
     *
     * @return int
     */
    public final int likeBuffer(int pBufferStartPos, int pBufferEndPos, int pCompStartPos, int pCompEndPos)
    {
        if (((pCompEndPos - pCompStartPos) == 0) && ((pBufferEndPos - pBufferStartPos) == 0))
        {
            if ((comparisonBuffer[pCompStartPos] == ANY) || (comparisonBuffer[pCompStartPos] == MORE))
            {
                return 0;
            }

            if (comparisonBuffer[pCompStartPos] == buffer[pBufferStartPos])
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
            else if (this.comparisonBuffer[i] == ANY)
            {
                j++;
            }

            // We have a '*' wildcard, check for
            // a match in the tail
            else if (this.comparisonBuffer[i] == MORE)
            {
                for (int f = j; f < (pBufferEndPos + 1); f++)
                {
                    if (likeBuffer(f, pBufferEndPos, i + 1, pCompEndPos) == 0)
                    {
                        return 0;
                    }
                }

                return -1;
            }

            // Both characters match, case insensitive
            else if ((j < (pBufferEndPos + 1)) && (comparisonBuffer[i] != buffer[j]))
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
            if ((pos <= pLen) && (pParamName[pos] != buffer[i]))
            {
                return (-1);
            }
            else if ((pos > pLen) && (pos <= (pLen + sepLen)) && (pValueSeperator[pos - (pLen + 1)] != buffer[i]))
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
     * @param pURLString java.lang.String
     *
     * @return java.lang.String
     */
    public final String parseHTTPRequest(String pHTTPRequest, int start, int limit)
    {
        return (parseHTTPRequest(pHTTPRequest, start, limit, true, true, true));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:56:12 PM)
     * @param pRemoveNewLines TODO
     * @param pURLString java.lang.String
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

        protocol = null;
        host = null;
        port = -1;
        file = null;
        ref = null;

        if (buffer == null)
        {
            if (buffersize < maxLength)
            {
                buffer = new char[maxLength];
            }
            else
            {
                buffer = new char[buffersize];
            }
        }
        else if (buffersize < maxLength)
        {
            buffersize = maxLength;
            buffer = new char[buffersize];
        }

        char[] s = pHTTPRequest.toCharArray();

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
                    buffer[p] = '_';
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
                        
                        buffer[p] = newc;

                        // if hex creates whitespace create an underscore instead
                        if (ch < 33)
                        {
                            buffer[p] = '_';
                        }

                        i += 2;

                        if (buffer[p] == ';')
                        {
                            buffer[p] = '?';
                        }

                        if ((buffer[p] == '%') && (scanAgain == false))
                        {
                            scanAgain = true;
                        }
                    }
                    catch (NumberFormatException e)
                    {
                        // throw new IllegalArgumentException();
                        buffer[p] = c;
                    }
                    catch (StringIndexOutOfBoundsException e)
                    {
                        // throw new IllegalArgumentException();
                        buffer[p] = c;
                    }

                    p++;

                    break;

                // change all ? into semicolons
                case ';':
                    buffer[p] = '?';
                    p++;

                    break;

                default:
                    buffer[p] = c;
                    p++;

                    break;
                }
            }
            else
            {
                // just duplicate
                buffer[i] = s[i];
                p = i;
            }

            pos = p - 1;

            // if slash is found then directory has been found
            if (buffer[pos] == '/')
            {
               
                this.lastDir = pos;

                // if dirStart = -1 then first slash is directory start
                if (this.dirStart == -1)
                {
                    this.dirStart = pos;
                    
                    if (pos > 0)
                    {
                        if (buffer[pos - 1] == ' ')
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
            else if ((fileEnd == -1) && ((buffer[pos] == ';') || (buffer[pos] == '?')))
            {
                this.dirEnd = this.lastDir;
                this.fileEnd = pos - 1;
                this.fileStart = lastDir + 1;

                if (this.fileEnd < this.fileStart)
                {
                    this.fileEnd = this.fileStart;
                }

                this.queryStart = pos + 1;

                // if ' ' has been found then url end has been found
            }
            else if ((buffer[pos] == ' ') && (this.lastDir != -1))
            {
                if (this.dirEnd == -1)
                {
                    this.dirEnd = lastDir;
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
            this.parseHTTPRequest(new String(buffer, 0, this.bufferEnd), start, maxLength, decodeMIME, false, pRemoveNewLines);
        }

        if (pReturnString == true)
        {
            return (new String(buffer, 0, this.bufferEnd));
        }

        return null;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:56:12 PM)
     *
     * @param pURLString java.lang.String
     *
     * @return java.lang.String
     */
    public final String parseURL(String pURL, int start, int limit)
    {
        return (parseURL(pURL, start, limit, true, true));
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 12:56:12 PM)
     *
     * @param pURLString java.lang.String
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

        protocol = null;
        host = null;
        port = -1;
        file = null;
        ref = null;

        if (buffer == null)
        {
            if (buffersize < maxLength)
            {
                buffer = new char[maxLength];
            }
            else
            {
                buffer = new char[buffersize];
            }
        }
        else if (buffersize < maxLength)
        {
            buffersize = maxLength;
            buffer = new char[buffersize];
        }

        char[] s = pURL.toCharArray();

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
                    buffer[p] = ' ';
                    p++;

                    break;

                case '%':

                    try
                    {
                        // if % but pos of %+3 greater than length then ignore else parse
                        if ((i + 3) <= len)
                        {
                            buffer[p] = ((char) Integer.parseInt(pURL.substring(i + 1, i + 3), 16));
                            i += 2;

                            if (buffer[p] == ';')
                            {
                                buffer[p] = '?';
                            }
                        }
                    }
                    catch (NumberFormatException e)
                    {
                        // throw new IllegalArgumentException();
                        buffer[p] = c;
                    }

                    p++;

                    break;

                // change all ? into semicolons
                case ';':
                    buffer[p] = '?';
                    p++;

                    break;

                default:
                    buffer[p] = c;
                    p++;

                    break;
                }
            }
            else
            {
                // just duplicate
                buffer[i] = s[i];
                p = i;
            }

            pos = p - 1;

            // if slash is found then directory has been found
            if ((this.protocolStart == -1) && (buffer[pos] == 'h') &&
                    ((buffer[pos + 1] == 't') & (buffer[pos + 2] == 't')))
            {
                this.protocolStart = pos;
            }
            else if ((this.protocolStart != -1) && (buffer[pos] == '/') && (buffer[pos - 1] == '/'))
            {
                this.hostNameStart = pos + 1;

                if (this.protocolStart != -1)
                {
                    this.protocolEnd = pos - 1;
                }

                // if ';' has been found query parameters have been found
            }
            else if ((this.hostNameStart != -1) && (buffer[pos] == '/'))
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
            else if ((fileEnd == -1) && ((buffer[pos] == ';') || (buffer[pos] == '?')))
            {
                this.dirEnd = this.lastDir;

                this.fileEnd = pos - 1;

                if (this.fileEnd < this.fileStart)
                {
                    this.fileEnd = this.fileStart;
                }

                this.fileStart = lastDir + 1;
                this.queryStart = pos + 1;

                // if ' ' has been found then url end has been found
            }
        }

        bufferEnd = p;

        if (this.dirEnd == -1)
        {
            this.dirEnd = lastDir;
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
            return (new String(buffer, 0, this.bufferEnd));
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
        PageParserDefinitions = newPageParserDefinitions;
    }

    /**
     * Insert the method's description here. Creation date: (5/12/2002 5:41:07 PM) Builds an index on the parameters in
     * the query seperator by ? and &
     */
    private final void tokenizeParameters()
    {
        int PPos = 0;

        if (queryEnd == -1)
        {
            this.ParameterPositions[PPos] = -1;

            return;
        }

        // otherwise set 0 to default to query end;
        this.ParameterPositions[PPos] = queryEnd;
        this.ParameterPositions[PPos + 1] = -1;

        // String str = new String(buffer,queryStart,queryEnd);
        for (int i = queryStart; i <= queryEnd; i++)
        {
            switch (buffer[i])
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
            this.ParameterPositions[PPos] = queryEnd;
            this.ParameterPositions[PPos + 1] = -1;
        }
    }
}
