import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.ETLJobExecutor;
import com.kni.etl.OSJobExecutor;
import com.kni.etl.SQLJobExecutor;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.KETLJobExecutor;
import com.kni.etl.sessionizer.XMLSessionizeJobExecutor;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ArgumentParserUtil;

public class RunJob {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        RunJob rj = new RunJob();

        rj.execute(args);

    }

    private void execute(String[] args) throws Exception {

       
        // declare XML filename
        String fileName = null;

        // declare job name override
        String jobID = null;


        // extract login information for metadata and xml filename
        for (int index = 0; index < args.length; index++) {

            if ((fileName == null) && (args[index].indexOf("FILE=") != -1)) {
                fileName = ArgumentParserUtil.extractArguments(args[index], "FILE=");
            }
        }

        // if filename is null report error
        if (fileName == null) {
            System.out
                    .println("Wrong arguments:  FILE=<XML_FILE> (SERVER=localhost) (JOB_NAME=<NAME>) (PARAMETER=[(TestList),PATH,/u01]) (IGNOREQA=[FileTest,SizeTest])");
            System.out.println("example:  FILE=c:\\transform.xml JOB_NAME=Transform SERVER=localhost");

            System.exit(-1);
        }

        // metadata object isn't set and login information found then connect to metadata

        ETLJobExecutor kJobExec = new KETLJobExecutor();

        ETLJobExecutor osJobExec = new OSJobExecutor();

        ETLJobExecutor sessionJobExec = new XMLSessionizeJobExecutor();

        ETLJobExecutor sqlJobExec = new SQLJobExecutor();

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Executing file " + fileName);
        Document doc;
        doc = XMLHelper.readXMLFromFile(fileName);

        if (doc == null)
            throw new RuntimeException("File \'" + fileName + "\' not found");

        NodeList nl = doc.getElementsByTagName("JOB");

        for (int i = 0; i < nl.getLength(); i++) {
            Node nd = nl.item(i);

            jobID = XMLHelper.getAttributeAsString(nd.getAttributes(), "ID", null);
            if (jobID == null)
                throw new RuntimeException("Job does not contain an ID attribute, aborting..");

            String type = XMLHelper.getAttributeAsString(nd.getAttributes(), "TYPE", null);
            ETLJobExecutor cur = null;
            if (type.equals("KETL")) {
                cur = kJobExec;
            }
            else if (type.equals("SQL")) {
                cur = sqlJobExec;
            }
            else if (type.equals("OSJOB")) {
                cur = osJobExec;
            }
            else if (type.equals("XMLSESSIONIZER")) {
                cur = sessionJobExec;
            }
            else if (type.equals("EMPTYJOB")) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Skipping empty job "
                        + jobID);
            }

            if (cur == null)
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                        "Unknown job type, skipping job " + jobID);
            else {

                String[] vargs = new String[args.length + 1];

                System.arraycopy(args, 0, vargs, 0, args.length);
                vargs[vargs.length - 1] = "JOBID=" + jobID;
                ETLJobExecutor._execute(vargs, cur, false, 0);
            }

        }

    }

}
