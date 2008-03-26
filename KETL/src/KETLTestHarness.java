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
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class KETLTestHarness.
 */
public class KETLTestHarness {

    /**
     * The Class TestJob.
     */
    static class TestJob {

        /** The type. */
        String id, type;
        
        /** The filename. */
        String filename;
        
        /** The depends. */
        ArrayList<String> depends = new ArrayList<String>();
    }

    /**
     * Suite.
     * 
     * @return the test
     */
    public static Test suite() {
        int iLoadID = (int) System.currentTimeMillis() / 2;

        TestSuite suite = new TestSuite("KETL Job Test: " + iLoadID);

        String testDir = System.getProperty("KETLTestDir");
        if (testDir == null) {
            testDir = "xml" + File.separator + "tests";
        }

        String treatAllAsEmpty = System.getProperty("KETLDependencieCheckOnly");

        System.out.println("Testing files found in \"" + testDir
                + "\" this can be changed by setting system property KETLTestDir.");

        ArrayList<TestJob> jobs = new ArrayList<TestJob>();
        HashSet<String> submittedJobs = new HashSet<String>();

        KETLTestHarness.suite(testDir, jobs);

        // dependencie consistency check
        HashSet<String> dependsCheck = new HashSet<String>();
        for (Object o : jobs) {
            TestJob tJob = (TestJob) o;
            dependsCheck.add(tJob.id);
        }

        while (jobs.size() > 0) {
            int size = jobs.size();
            Object[] tmp = jobs.toArray();
            for (Object o : tmp) {
                TestJob tJob = (TestJob) o;

                boolean in = true;
                for (int i = 0; i < tJob.depends.size(); i++) {
                    if (dependsCheck.contains(tJob.depends.get(i)) == false) {
                        System.err.println("[" + new java.util.Date() + "] Job \"" + tJob.id + "\" depends on job \"" + tJob.depends.get(i)
                                + "\" which does not exist in the set of jobs being tested");
                        tJob.depends.remove(i);
                    }
                    else if (submittedJobs.contains(tJob.depends.get(i)) == false)
                        in = false;
                }

                if (in) {
                    System.out.print("Job " + tJob.id + " depends On:\t");
                    for (int d = 0; d < tJob.depends.size(); d++) {
                        if (d > 0)
                            System.out.print(", ");
                        System.out.print(tJob.depends.get(d).toString());
                    }
                    System.out.println("");

                    suite.addTest(new KETLTestWrapper(tJob.filename, tJob.id, treatAllAsEmpty == null ? tJob.type
                            : "EMPTYJOB", iLoadID));
                    jobs.remove(tJob);
                    submittedJobs.add(tJob.id);
                }
            }

            if (jobs.size() == size)
                throw new RuntimeException("Job dependendencies are not complete");
        }

        return suite;
    }

    /**
     * Suite.
     * 
     * @param testDir the test dir
     * @param jobs the jobs
     */
    public static void suite(String testDir, ArrayList<TestJob> jobs) {

        File dir = new File(testDir);

        if (dir.exists() == false || dir.isDirectory() == false)
            throw new RuntimeException("Test directory is invalid: " + testDir);

        String children[] = dir.list();
        for (String element : children) {
            File fl = new File(dir.getAbsolutePath() + File.separator + element);

            if (fl.isDirectory())
                KETLTestHarness.suite(fl.getAbsolutePath(), jobs);
            else if (element.endsWith(".xml")) {
                try {
                    Document doc = XMLHelper.readXMLFromFile(fl.getAbsolutePath());

                    NodeList nl = doc.getElementsByTagName("JOB");

                    for (int x = 0; x < nl.getLength(); x++) {
                        String jobid = XMLHelper.getAttributeAsString(nl.item(x).getAttributes(), "ID", null);
                        if (jobid == null)
                            throw new Exception("Job tag found but ID attribute not found, skipping");

                        TestJob job = new TestJob();
                        job.id = jobid;
                        job.filename = testDir + File.separator + element;
                        Node[] depends = XMLHelper.getElementsByName(nl.item(x), "DEPENDS_ON", null, null);

                        if (depends != null)
                            for (Node element0 : depends)
                                job.depends.add(XMLHelper.getTextContent(element0));

                        depends = XMLHelper.getElementsByName(nl.item(x), "WAITS_ON", null, null);
                        if (depends != null)
                            for (Node element0 : depends)
                                job.depends.add(XMLHelper.getTextContent(element0));

                        job.type = XMLHelper.getAttributeAsString(nl.item(x).getAttributes(), "TYPE", null);

                        if (job.type == null)
                            throw new Exception("Job type is null, check job " + jobid);

                        jobs.add(job);
                    }
                } catch (Exception e) {
                    System.err.println("[" + new java.util.Date() + "] Skipping file " + element + ", Error:" + e.getMessage());
                }
            }
        }

    }

}
