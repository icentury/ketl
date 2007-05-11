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
/*
 * Created on Aug 15, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.stringtools;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

// TODO: Auto-generated Javadoc
/**
 * The Class DesEncrypter.
 */
public class DesEncrypter {

    /** The ecipher. */
    Cipher ecipher;
    
    /** The dcipher. */
    Cipher dcipher;

    // 8-byte Salt
    /** The salt. */
    byte[] salt = { (byte) 0xA9, (byte) 0x9B, (byte) 0xC8, (byte) 0x32, (byte) 0x56, (byte) 0x35, (byte) 0xE3,
            (byte) 0x03 };

    // Iteration count
    /** The iteration count. */
    int iterationCount = 19;

    /**
     * Instantiates a new des encrypter.
     * 
     * @param passPhrase the pass phrase
     * 
     * @throws Exception the exception
     */
    public DesEncrypter(String passPhrase) throws Exception {
        // Create the key
        KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), this.salt, this.iterationCount);
        SecretKey key = SecretKeyFactory.getInstance("PBEWithMD5AndDES").generateSecret(keySpec);
        this.ecipher = Cipher.getInstance(key.getAlgorithm());
        this.dcipher = Cipher.getInstance(key.getAlgorithm());

        // Prepare the parameter to the ciphers
        AlgorithmParameterSpec paramSpec = new PBEParameterSpec(this.salt, this.iterationCount);

        // Create the ciphers
        this.ecipher.init(Cipher.ENCRYPT_MODE, key, paramSpec);
        this.dcipher.init(Cipher.DECRYPT_MODE, key, paramSpec);
    }

    /**
     * Encrypt.
     * 
     * @param str the str
     * 
     * @return the string
     * 
     * @throws UnsupportedEncodingException the unsupported encoding exception
     * @throws IllegalBlockSizeException the illegal block size exception
     * @throws BadPaddingException the bad padding exception
     */
    public String encrypt(String str) throws UnsupportedEncodingException, IllegalBlockSizeException, BadPaddingException  {
        // Encode the string into bytes using utf-8
        byte[] utf8 = str.getBytes("UTF8");

        // Encrypt
        byte[] enc = this.ecipher.doFinal(utf8);

        // Encode bytes to base64 to get a string
        return new sun.misc.BASE64Encoder().encode(enc);
    }

    /**
     * Decrypt.
     * 
     * @param str the str
     * 
     * @return the string
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws IllegalBlockSizeException the illegal block size exception
     * @throws BadPaddingException the bad padding exception
     */
    public String decrypt(String str) throws IOException, IllegalBlockSizeException, BadPaddingException {
        // Decode base64 to get bytes
        byte[] dec = new sun.misc.BASE64Decoder().decodeBuffer(str);

        // Decrypt
        byte[] utf8 = this.dcipher.doFinal(dec);

        // Decode using utf-8
        return new String(utf8, "UTF8");
    }
    
   
}
