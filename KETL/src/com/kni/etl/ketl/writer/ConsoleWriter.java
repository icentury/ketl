/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;

/**
 * <p>
 * Title: ETLWriter
 * </p>
 * <p>
 * Description: Abstract base class for ETL destination loading.
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
public class ConsoleWriter extends ETLWriter implements DefaultWriterCore {

    public ConsoleWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    private boolean listHeaders = true;
    private OutputStreamWriter osw = new OutputStreamWriter(System.out);
    private PrintWriter pw = new PrintWriter(this.osw, true);

    public int putNextRecord(Object[] o, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

        if (this.listHeaders) {
            for (int i = 0; i < this.mInPorts.length; i++) {
                if (i > 0)
                    this.pw.print(';');
                this.pw.print(this.mInPorts[i].mstrName + "(" + this.mInPorts[i].getPortClass().getCanonicalName()
                        + ")");
            }
            this.pw.println();
            this.listHeaders = false;
        }
        for (int i = 0; i < this.mInPorts.length; i++) {
            if (i > 0)
                this.pw.print(';');

            Object data = this.mInPorts[i].isConstant() ? this.mInPorts[i].getConstantValue() : o[this.mInPorts[i]
                    .getSourcePortIndex()];

            if (data == null)
                this.pw.print("[NULL]");
            else if (this.mInPorts[i].isArray()) {
                Object[] ar = (Object[]) data;
                this.pw.print(java.util.Arrays.toString(ar));
            }
            else
                this.pw.print(data);

        }
        this.pw.println();
        return 1;
    }

    @Override
    protected void close(boolean success) {

    }

}
