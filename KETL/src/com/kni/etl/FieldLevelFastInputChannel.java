/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.ParsePosition;
import java.util.zip.GZIPInputStream;

import com.kni.etl.stringtools.FastSimpleDateFormat;

public final class FieldLevelFastInputChannel {

    char[] mTempBuffer;
    private ReadableByteChannel miStream;
    Reader mWorkBuffer;
    boolean EOF = false;

    /**
     * Creates a new FieldLevelFastInputChannel object.
     * 
     * @param iStream DOCUMENT ME!
     * @param mode DOCUMENT ME!
     * @param bufsize DOCUMENT ME!
     * @param ioType DOCUMENT ME!
     * @throws Exception DOCUMENT ME!
     */
    public FieldLevelFastInputChannel(ReadableByteChannel iStream, String mode, int bufsize) throws Exception {
        this(iStream, mode, bufsize, null, CodingErrorAction.REPORT);
    }

    /**
     * Creates a new FieldLevelFastInputChannel object.
     * 
     * @param iStream DOCUMENT ME!
     * @param mode DOCUMENT ME!
     * @param bufsize DOCUMENT ME!
     * @param pCharSet DOCUMENT ME!
     * @param pZipped Is file compressed
     * @param pCodingErrorException TODO
     * @param pDelimiter DOCUMENT ME!
     * @param pTreatReturnAsLineEnd DOCUMENT ME!
     * @param ioType DOCUMENT ME!
     * @throws Exception DOCUMENT ME!
     */
    public FieldLevelFastInputChannel(ReadableByteChannel iStream, String mode, int bufsize, String pCharSet,
            boolean pZipped, CodingErrorAction pCodingErrorException) throws Exception {
        this.miStream = iStream;

        if (pCharSet == null) {
            pCharSet = Charset.defaultCharset().displayName();
        }

        CharsetDecoder dec = Charset.forName(pCharSet).newDecoder();
        dec.onMalformedInput(pCodingErrorException);
        dec.onUnmappableCharacter(pCodingErrorException);

        if (pZipped) {
            this.mInputStream = Channels.newInputStream(iStream);
            this.mGZipStream = new GZIPInputStream(mInputStream);
            this.mWorkBuffer = new InputStreamReader(mGZipStream, Charset.forName(pCharSet).newDecoder());
        }
        else { // decode and read from the file channel
            this.mWorkBuffer = Channels.newReader(iStream, Charset.forName(pCharSet).newDecoder(), -1);
        }
        // create a local buffer to prevent having to read buffer char by char
        // buffered reader wasn't as good.
        this.mTempBuffer = new char[bufsize];
    }

    /**
     * Creates a new FieldLevelFastInputChannel object.
     * 
     * @param iStream DOCUMENT ME!
     * @param mode DOCUMENT ME!
     * @param bufsize DOCUMENT ME!
     * @param pCharSet DOCUMENT ME!
     * @param pCodingErrorException TODO
     * @param pDelimiter DOCUMENT ME!
     * @param pTreatReturnAsLineEnd DOCUMENT ME!
     * @param ioType DOCUMENT ME!
     * @throws Exception DOCUMENT ME!
     */
    public FieldLevelFastInputChannel(ReadableByteChannel iStream, String mode, int bufsize, String pCharSet,
            CodingErrorAction pCodingErrorException) throws Exception {
        this(iStream, mode, bufsize, pCharSet, false, pCodingErrorException);
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME!
     */
    public boolean isEndOfFile() {
        return this.EOF;
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws IOException DOCUMENT ME!
     */
    public final void close() throws IOException {
        if (this.mInputStream != null)
            this.mInputStream.close();

        if (this.mGZipStream != null) {
            this.mGZipStream.close();
        }

        if (this.mWorkBuffer != null)
            this.mWorkBuffer.close();

        if (this.miStream != null)
            this.miStream.close();

    }

    public final static int END_OF_FILE = -1;
    public final static int BUFFER_TO_SMALL = -2;

    private int charsRead = 0;
    private int pos = 0;

    // us this little method to prevent calling synchronized method all the time
    // marginal performance improvement
    final private int read() throws IOException {
        if (pos == charsRead) {
            pos = 0;
            charsRead = this.mWorkBuffer.read(this.mTempBuffer);
            if (charsRead == -1)
                return -1;
        }

        return this.mTempBuffer[pos++];
    }

    // us this little method to prevent calling synchronized method all the time
    // marginal performance improvement
    final private int read(char[] pBuf, int pStart, int pLength) throws IOException {
        // bulk copy if possible
        int fetchSize = charsRead - pos > pLength ? pLength : charsRead - pos;
        int readChars = 0;

        if (fetchSize > 0) {
            System.arraycopy(this.mTempBuffer, pos, pBuf, pStart, fetchSize);
            pStart += fetchSize;
            pLength -= fetchSize;
            pos += fetchSize;
            readChars = fetchSize;
        }

        for (int i = pStart; i < pStart + pLength; i++) {
            int ch = read();
            if (ch == -1) {
                return readChars == 0 ? -1 : readChars;
            }

            readChars++;
            pBuf[i] = (char) ch;
        }

        return readChars;
    }

    private boolean mbAllowForNoDelimeterAtEOF;

    public void allowForNoDelimeterAtEOF(boolean arg0) {
        this.mbAllowForNoDelimeterAtEOF = arg0;
    }

    public final int readDelimitedField(char[] pDelimiter, char[] quoteStart, char[] quoteEnd,
            boolean escapeDoubleQuotes, Character pEscapeChar, int pFieldMaxLength, int pAvgSize, char[] pOutput,
            boolean pAutoTruncate) throws Exception {
        return this.readDelimitedField(pDelimiter, quoteStart, quoteEnd, escapeDoubleQuotes, pEscapeChar,
                pFieldMaxLength, pAvgSize, pOutput, pAutoTruncate, false);
    }

    /**
     * return a next line in passed byte array
     * 
     * @param pAutoTruncate TODO
     * @param pCopy TODO
     */
    public final int readDelimitedField(char[] pDelimiter, char[] quoteStart, char[] quoteEnd,
            boolean escapeDoubleQuotes, Character pEscapeChar, int pFieldMaxLength, int pAvgSize, char[] pOutput,
            boolean pAutoTruncate, boolean pKeepDelimiter) throws Exception {
        int size = 0;
        char delimiterFirstChar = pDelimiter[0];
        boolean hasQuotes = ((quoteStart == null) || (quoteEnd == null)) ? false : true;
        boolean quotesOpen = false;
        boolean previousValueWasQuote = false;
        int pos = 0;

        // average size has to be greater than 0
        if (pAvgSize < 1) {
            pAvgSize++;
        }

        if (EOF) {
            throw new EOFException();
        }

        if (pOutput.length < ((quoteStart == null ? 0 : quoteStart.length) + pFieldMaxLength + (quoteEnd == null ? 0
                : quoteEnd.length))) {
            return BUFFER_TO_SMALL;
        }
        do {
            while (pos < pFieldMaxLength) {

                // get current char
                int ch = this.read();
                if (ch == -1) {
                    EOF = true;
                    if (mbAllowForNoDelimeterAtEOF)
                        return pos;
                    throw new EOFException();
                }

                // if escape char then loop to next char
                if (pEscapeChar != null && ch == pEscapeChar.charValue()) {
                    ch = this.read();
                    if (ch == -1) {
                        EOF = true;
                        if (mbAllowForNoDelimeterAtEOF)
                            return pos;
                        throw new EOFException();
                    }
                    pOutput[pos++] = (char) ch;

                    continue;
                }

                // look for opening quote if required
                if (hasQuotes && (quotesOpen == false) && (ch == quoteStart[0])) {

                    int quoteLength = quoteStart.length;
                    boolean foundQuote = true;
                    int recordPos = pos;
                    pOutput[pos++] = (char) ch;

                    // check full length of quote for match
                    for (int x = 1; x < quoteLength; x++) {
                        ch = this.read();
                        if (ch == -1) {
                            EOF = true;
                            throw new EOFException();
                        }
                        pOutput[pos++] = (char) ch;
                        if (ch != quoteStart[x]) {
                            x = quoteLength;
                            foundQuote = false;
                        }
                    }

                    if (foundQuote) {
                        if (escapeDoubleQuotes) {
                            if (previousValueWasQuote)
                                previousValueWasQuote = false;
                            else
                                pos = recordPos;
                        }
                        else
                            pos = recordPos;
                        quotesOpen = true;
                    }
                }

                // look for closing quotes
                else if ((quotesOpen == true) && (ch == quoteEnd[0])) {
                    boolean foundQuote = true;
                    int recordPos = pos;
                    pOutput[pos++] = (char) ch;

                    int quoteLength = quoteEnd.length;

                    for (int x = 1; x < quoteLength; x++) {
                        ch = this.read();
                        if (ch == -1) {
                            EOF = true;
                            throw new EOFException();
                        }
                        pOutput[pos++] = (char) ch;
                        if (ch != quoteEnd[x]) {
                            x = quoteLength;
                            foundQuote = false;
                        }
                    }

                    if (foundQuote) {
                        pos = recordPos;
                        quotesOpen = false;
                    }

                    previousValueWasQuote = true;
                }
                else if ((quotesOpen == false) && (ch == delimiterFirstChar)) {

                    int delimiterLength = pDelimiter.length;
                    int recordPos = pos;
                    boolean foundDelimiter = true;

                    if (delimiterLength == 0 && pKeepDelimiter)
                        pOutput[pos++] = (char) ch;

                    if (delimiterLength > 1) {
                        pOutput[pos++] = (char) ch;

                        for (int x = 1; x < delimiterLength; x++) {
                            ch = this.read();
                            if (ch == -1) {
                                EOF = true;
                                throw new EOFException();
                            }
                            pOutput[pos++] = (char) ch;
                            if (ch != pDelimiter[x]) {
                                x = delimiterLength;
                                foundDelimiter = false;
                            }
                        }

                    }

                    if (foundDelimiter) {
                        pos = recordPos;

                        if (pKeepDelimiter)
                            return pos + delimiterLength;

                        return pos;
                    }
                }
                else
                    pOutput[pos++] = (char) ch;

                size++;
            }
            pos--;
        } while (pAutoTruncate);

        throw new Exception("Field longer than max specified length, max length=" + pFieldMaxLength
                + ", increase max field length or enable auto truncate");
    }

    /**
     * return a next line in passed byte array
     * 
     * @param pCopy TODO
     */
    public final int readFixedLengthField(int pFieldLength, char[] quoteStart, char[] quoteEnd, char[] pOutput)
            throws Exception {
        int res;
        boolean hasQuotes = ((quoteStart == null) || (quoteEnd == null)) ? false : true;

        if (EOF) {
            throw new EOFException();
        }

        res = this.read(pOutput, 0, pFieldLength);

        if (res == -1) {
            EOF = true;
            throw new EOFException();
        }

        if (res != pFieldLength) {
            EOF = true;
            throw new IOException("Requested field length greater than remaining characters, remaining = " + res
                    + ", request = " + pFieldLength);
        }

        if (hasQuotes & res > 0) {
            int newLength = res;
            int startQuoteLength = quoteStart.length;
            boolean startQuotesFound = true;
            // look for opening quote if required
            for (int x = 0; x < startQuoteLength && x < res; x++) {
                if (pOutput[x] != quoteStart[x]) {
                    x = startQuoteLength;
                    startQuotesFound = false;
                }
            }

            if (startQuotesFound)
                newLength = newLength - startQuoteLength;

            int endQuoteLength = quoteEnd.length;
            boolean endQuotesFound = true;
            // look for opening quote if required
            for (int x = res - endQuoteLength; x < res && x >= 0; x++) {
                if (pOutput[x] != quoteEnd[x]) {
                    x = endQuoteLength;
                    endQuotesFound = false;
                }
            }

            if (endQuotesFound)
                newLength = newLength - endQuoteLength;

            if (endQuotesFound || startQuotesFound) {
                System.arraycopy(pOutput, (startQuotesFound ? startQuoteLength : 0), pOutput, 0, endQuoteLength);
            }

            res = endQuoteLength;

        }

        return res;
    }

    public static final int MAXFIELDLENGTH = 256;
    private InputStream mInputStream;
    private GZIPInputStream mGZipStream;

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws UnsupportedEncodingException
     * @throws UnsupportedEncodingException
     */
    public final static byte[] toByte(char[] arg0, int len, String charSet) throws UnsupportedEncodingException {
        if (len == 0) {
            return null;
        }

        if (charSet == null) {
            return (new String(arg0, 0, len)).getBytes();
        }

        return (new String(arg0, 0, len)).getBytes(charSet);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static Boolean toBoolean(char[] arg0, int len) {
        if (len == 0) {
            return null;
        }

        return Boolean.parseBoolean(new String(arg0, 0, len));
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static byte[] toByteArray(char[] arg0, int len) {
        if (len == 0) {
            return null;
        }

        return new String(arg0, 0, len).getBytes();
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static Character toChar(char[] arg0, int len) {
        if (len == 0) {
            return null;
        }

        return Character.valueOf(arg0[0]);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static Double toDouble(char[] arg0, int len) throws NumberFormatException {
        if (len == 0) {
            return null;
        }

        String str = new String(arg0, 0, len).trim();
        if (str.length() == 0)
            return null;

        return Double.parseDouble(str);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @param formatter DOCUMENT ME!
     * @param position DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static java.util.Date toDate(char[] arg0, int len, FastSimpleDateFormat formatter,
            ParsePosition position) throws Exception {
        if (len == 0) {
            return null;
        }

        return formatter.parse(new String(arg0, 0, len), position);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static Integer toInteger(char[] arg0, int len) throws NumberFormatException {
        if (len == 0) {
            return null;
        }

        String str = new String(arg0, 0, len).trim();
        if (str.length() == 0)
            return null;
        return Integer.parseInt(str);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static Short toShort(char[] arg0, int len) throws NumberFormatException {
        if (len == 0) {
            return null;
        }

        String str = new String(arg0, 0, len).trim();
        if (str.length() == 0)
            return null;

        return Short.parseShort(str);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static Long toLong(char[] arg0, int len) throws NumberFormatException {
        if (len == 0) {
            return null;
        }

        String str = new String(arg0, 0, len).trim();
        if (str.length() == 0)
            return null;

        return Long.parseLong(str);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static Float toFloat(char[] arg0, int len) throws NumberFormatException {
        if (len == 0) {
            return null;
        }

        String str = new String(arg0, 0, len).trim();
        if (str.length() == 0)
            return null;

        return Float.parseFloat(str);
    }

    public final static char[] toCharArray(char[] arg0, int len) {
        if (len == 0) {
            return null;
        }

        char[] ch = new char[len];
        System.arraycopy(arg0, 0, ch, 0, len);
        return ch;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param arg0 DOCUMENT ME!
     * @param len DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    public final static String toString(char[] arg0, int len) {
        if (len == 0) {
            return null;
        }
        return new String(arg0, 0, len);

    }

}
