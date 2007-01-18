/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

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
public class ExcelWriter extends ETLWriter implements DefaultWriterCore {

    private int mTabPortIndex = -1;
    private BufferedWriter xmlOut;
    private PrintWriter out;

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        // TODO Auto-generated method stub
        int res = super.initialize(xmlConfig);

        if (res != 0)
            return res;

        for (int i = 0; i < this.mInPorts.length; i++) {
            if (XMLHelper.getAttributeAsBoolean(this.mInPorts[i].getXMLConfig().getAttributes(), "TAB", false)) {
                this.mTabPortIndex = i;
            }
        }

        String filePath = this.getParameterValue(0, "FILEPATH");

        File fn;
        try {
            fn = new File(filePath);

            // stream the file to the browser for download
            out = new PrintWriter(fn);

            xmlOut = new BufferedWriter(out);

            writeData("<?xml version=\"1.0\"?>\n" + "<?mso-application progid=\"Excel.Sheet\"?>"
                    + "<ss:Workbook xmlns=\"urn:schemas-microsoft-com:office:spreadsheet\" "
                    + "xmlns:o=\"urn:schemas-microsoft-com:office:office\" "
                    + "xmlns:x=\"urn:schemas-microsoft-com:office:excel\"  "
                    + "xmlns:ss=\"urn:schemas-microsoft-com:office:spreadsheet\"  "
                    + "xmlns:html=\"http://www.w3.org/TR/REC-html40\">");
            writeData("<ss:Styles><Style ss:ID=\"Default\" ss:Name=\"Normal\"><Font ss:Size=\"8\"/>"
                    + "</Style><ss:Style ss:ID=\"1\"><Borders><Border ss:Position=\"Bottom\""
                    + " ss:LineStyle=\"Continuous\" ss:Weight=\"1\"/></Borders></ss:Style>"
                    + "<Style ss:ID=\"s22\"><NumberFormat" + " ss:Format=\"m/d/yy\\ h:mm;@\"/>"
                    + "</Style>\n</ss:Styles>");

            if (this.mTabPortIndex == -1)
                this.createSheet("Results");
        } catch (IOException e) {
            throw new KETLThreadException(e, this);
        }
        return 0;

    }

    public ExcelWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        if (pPartition > 1)
            throw new KETLThreadException(
                    "Excel writer cannot be executed in parallel, add FLOWTYPE=\"FANIN\" to step definition", this);

    }

    private Object mCurrentTab = null;

    private void addHeaders() throws IOException {
        writeData("<ss:Row ss:StyleID=\"1\">");

        for (int i = 0; i < this.mInPorts.length; i++) {

            printCell(this.mInPorts[i].mstrName, String.class);
        }
        writeData("</ss:Row>");

    }

    public int putNextRecord(Object[] o, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

        try {
            if (this.mTabPortIndex != -1) {

                Object data = this.mInPorts[this.mTabPortIndex].isConstant() ? this.mInPorts[this.mTabPortIndex]
                        .getConstantValue() : o[this.mInPorts[this.mTabPortIndex].getSourcePortIndex()];
                if (mCurrentTab == null || mCurrentTab.equals(data) == false) {

                    if (this.mCurrentTab != null) {
                        this.closeSheet();
                    }
                    mCurrentTab = data;
                    this.createSheet(data.toString());
                }
            }

            writeData("<ss:Row>");
            for (int i = 0; i < this.mInPorts.length; i++) {

                Object data = this.mInPorts[i].isConstant() ? this.mInPorts[i].getConstantValue() : o[this.mInPorts[i]
                        .getSourcePortIndex()];

                this.printCell(data, this.mInPorts[i].getPortClass());
            }

            writeData("</ss:Row>");
        } catch (IOException e) {
            throw new KETLWriteException(e.getMessage());
        }

        return 1;
    }

    @Override
    protected void close(boolean success) {
        try {
            if (xmlOut != null) {
                xmlOut.close();
                xmlOut = null;
            }

            if (out != null) {
                out = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void writeData(String pData) throws IOException {
        xmlOut.write(pData);
    }

    void createSheet(String pName) throws IOException {
        writeData("<ss:Worksheet ss:Name=\"" + checkAndEscapeXMLData(pName) + "\"><ss:Table>");
        this.addHeaders();
    }

    SimpleDateFormat customDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    void printCell(Object pValue, Class pClass) throws IOException {

        if (pValue == null)
            writeData("<ss:Cell><ss:Data ss:Type=\"String\">");
        else if (pClass == String.class) {
            writeData("<ss:Cell><ss:Data ss:Type=\"String\">");
            writeData(checkAndEscapeXMLData((String)pValue));
        }
        else if (pClass == BigDecimal.class) {
            writeData("<ss:Cell><ss:Data ss:Type=\"Number\">");
            writeData(checkAndEscapeXMLData(Double.toString(((BigDecimal) pValue).doubleValue())));
        }
        else if (pClass == Timestamp.class) {
            writeData("<ss:Cell ss:StyleID=\"s22\"><ss:Data ss:Type=\"DateTime\">");
            writeData(checkAndEscapeXMLData(customDateFormat.format((Timestamp) pValue)));
        }
        else {
            writeData("<ss:Cell><ss:Data ss:Type=\"String\">");
            writeData(checkAndEscapeXMLData(pValue.toString()));
        }

        writeData("</ss:Data></ss:Cell>");
    }

    public static String padNull(String arg0) {
        if (arg0 == null || arg0.length() == 1)
            return "&nbsp;";

        return arg0;
    }

    void closeSheet() throws IOException {
        writeData("</ss:Table></ss:Worksheet>");
    }

    private boolean mDataLengthWarning = true;

    private String checkAndEscapeXMLData(String pXML) {

        if (pXML.length() > 255) {
            pXML = pXML.substring(0, 254);
            if (mDataLengthWarning) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Data length over 255, actual length "
                        + pXML.length());
                mDataLengthWarning = false;
            }
        }

        String str = pXML.replaceAll("&", "&amp;");
        str = pXML.replaceAll("\"", "&quot;");
        str = str.replaceAll("<", "&lt;");
        str = str.replaceAll(">", "&gt;");
        str = str.replaceAll("’", "&apos;");

        return str;
    }

    @Override
    public int complete() throws KETLThreadException {

        int res = super.complete();

        if (res != 0)
            return res;

        try {
            this.closeSheet();
            writeData("</ss:Workbook>");

            if (xmlOut != null) {
                xmlOut.close();
                xmlOut = null;
            }

            if (out != null) {
                out.flush();
                out = null;
            }
        } catch (IOException e) {
            throw new KETLThreadException(e, e.getMessage());
        }

        return 0;
    }

}
