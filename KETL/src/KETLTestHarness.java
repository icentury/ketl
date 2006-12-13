import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.util.XMLHelper;

public class KETLTestHarness {

    static class TestJob {

        String id, type;
        String filename;
        ArrayList depends = new ArrayList();
    }

    public static Test suite() {
        int iLoadID = (int) System.currentTimeMillis()/2;

        TestSuite suite = new TestSuite("KETL Job Test: " + iLoadID);

        String testDir = System.getProperty("KETLTestDir");
         if (testDir == null) {
            testDir = "xml" + File.separator + "tests";
        }

        String treatAllAsEmpty = System.getProperty("KETLDependencieCheckOnly");
        
        
         
        System.out.println("Testing files found in \"" + testDir
                + "\" this can be changed by setting system property KETLTestDir.");

        ArrayList jobs = new ArrayList();
        HashSet submittedJobs = new HashSet();

        suite(testDir, jobs);

        // dependencie consistency check
        HashSet dependsCheck = new HashSet();
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
                    if (dependsCheck.contains(tJob.depends.get(i)) == false){
                        System.err.println("Job \"" + tJob.id + "\" depends on job \"" + tJob.depends.get(i)
                                + "\" which does not exist in the set of jobs being tested");
                        tJob.depends.remove(i);
                    }
                    else if (submittedJobs.contains(tJob.depends.get(i)) == false)
                        in = false;
                }

                if (in) {   
                    System.out.print("Job "+ tJob.id + " depends On:\t");
                    for(int d=0;d<tJob.depends.size();d++){
                        if(d>0)
                            System.out.print(", ");
                        System.out.print(tJob.depends.get(d).toString());
                    }
                    System.out.println("");
                    
                    suite.addTest(new KETLTestWrapper(tJob.filename, tJob.id,treatAllAsEmpty==null?tJob.type:"EMPTYJOB", iLoadID));
                    jobs.remove(tJob);
                    submittedJobs.add(tJob.id);
                }
            }

            if (jobs.size() == size)
                throw new RuntimeException("Job dependendencies are not complete");
        }

        return suite;
    }

    public static void suite(String testDir, ArrayList jobs) {

        File dir = new File(testDir);

        if (dir.exists() == false || dir.isDirectory() == false)
            throw new RuntimeException("Test directory is invalid: " + testDir);

        String children[] = dir.list();
        for (int i = 0; i < children.length; i++) {
            File fl = new File(dir.getAbsolutePath() + File.separator + children[i]);

            if (fl.isDirectory())
                KETLTestHarness.suite(fl.getAbsolutePath(), jobs);
            else if (children[i].endsWith(".xml")) {
                try {
                    Document doc = XMLHelper.readXMLFromFile(fl.getAbsolutePath());

                    NodeList nl = doc.getElementsByTagName("JOB");

                    for (int x = 0; x < nl.getLength(); x++) {
                        String jobid = XMLHelper.getAttributeAsString(nl.item(x).getAttributes(), "ID", null);
                        if (jobid == null)
                            throw new Exception("Job tag found but ID attribute not found, skipping");

                        TestJob job = new TestJob();
                        job.id = jobid;
                        job.filename = testDir + File.separator + children[i];
                        Node[] depends = XMLHelper.getElementsByName(nl.item(x), "DEPENDS_ON", null, null);

                        if (depends != null)
                            for (int p = 0; p < depends.length; p++)
                                job.depends.add(XMLHelper.getTextContent(depends[p]));

                        depends = XMLHelper.getElementsByName(nl.item(x), "WAITS_ON", null, null);
                        if (depends != null)
                            for (int p = 0; p < depends.length; p++)
                                job.depends.add(XMLHelper.getTextContent(depends[p]));

                        job.type = XMLHelper.getAttributeAsString(nl.item(x).getAttributes(), "TYPE", null);

                        if (job.type == null)
                            throw new Exception("Job type is null, check job " + jobid);

                        jobs.add(job);
                    }
                } catch (Exception e) {
                    System.err.println("Skipping file " + children[i] + ", Error:" + e.getMessage());
                }
            }
        }

    }

}
