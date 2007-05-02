import junit.framework.TestCase;

import com.kni.etl.ETLJobExecutor;
import com.kni.etl.OSJobExecutor;
import com.kni.etl.SQLJobExecutor;
import com.kni.etl.ketl.KETLJobExecutor;

public class KETLTestWrapper extends TestCase {

    String filename;
    String jobid, type;
    int loadid;

    public KETLTestWrapper(String filename, String jobid, String type, int loadid) {
        super("testJob");
        this.jobid = jobid;
        this.filename = filename;
        this.loadid = loadid;
        this.type = type;
    }

    @Override
    public String getName() {
        return (this.jobid + " - " + this.filename + "");
    }

    public void testJob() throws Throwable {
        try {

            ETLJobExecutor cur = null;
            if (this.type.equals("KETL")) {
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
