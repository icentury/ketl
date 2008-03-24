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
import junit.framework.TestCase;

import com.kni.etl.ETLJobExecutor;
import com.kni.etl.OSJobExecutor;
import com.kni.etl.SQLJobExecutor;
import com.kni.etl.ketl.KETLJobExecutor;

// TODO: Auto-generated Javadoc
/**
 * The Class KETLTestWrapper.
 */
public class KETLTestWrapper extends TestCase {

    /** The filename. */
    String filename;
    
    /** The type. */
    String jobid, type;
    
    /** The loadid. */
    int loadid;

    /**
     * Instantiates a new KETL test wrapper.
     * 
     * @param filename the filename
     * @param jobid the jobid
     * @param type the type
     * @param loadid the loadid
     */
    public KETLTestWrapper(String filename, String jobid, String type, int loadid) {
        super("testJob");
        this.jobid = jobid;
        this.filename = filename;
        this.loadid = loadid;
        this.type = type;
    }

    /* (non-Javadoc)
     * @see junit.framework.TestCase#getName()
     */
    @Override
    public String getName() {
        return (this.jobid + " - " + this.filename + "");
    }

    /**
     * Test job.
     * 
     * @throws Throwable the throwable
     */
    public void testJob() throws Throwable {
        try {

            ETLJobExecutor cur = null;
            if (this.type.startsWith("KETL")) {
                cur = new KETLJobExecutor();
            }
            else if (this.type.equals("SQL")) {
                cur = new SQLJobExecutor();
            }
            else if (this.type.equals("OSJOB")) {
                cur = new OSJobExecutor();
            }
            else if (this.type.equals("EMPTYJOB")) {
                return;
            }

            if (cur == null)
                throw new RuntimeException("Unknown job type " + this.type);

            ETLJobExecutor._execute(new String[] { "FILE=" + this.filename, "JOBID=" + this.jobid,
                    "LOADID=" + this.loadid }, cur, false, this.loadid);
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof com.kni.etl.ketl.writer.ForcedException)
                return;
            throw e.getCause();
        }
    }
}
