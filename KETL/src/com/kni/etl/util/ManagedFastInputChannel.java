/**
 * 
 */
package com.kni.etl.util;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import com.kni.etl.FieldLevelFastInputChannel;

public class ManagedFastInputChannel {

    public ReadableByteChannel mfChannel;
    public int miCurrentRecord;
    public String mPath;
    public FieldLevelFastInputChannel mReader;

    public void close() throws IOException {
        mfChannel.close();
        if (this.mReader != null)
            this.mReader.close();
    }
}