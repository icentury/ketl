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
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

// TODO: Auto-generated Javadoc
/*
 * Created on Apr 7, 2005 To change the template for this generated file go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
/**
 * The Class KETLBootStrap.
 */
public class KETLBootStrap {

    /**
     * The main method.
     * 
     * @param args the args
     */
    public static void main(String[] args) {
        // Start up a KETL server in the background
        if ((args.length == 3) && args[2].equalsIgnoreCase("FOREGROUND")) {
            KETLBootStrap.startProcess(args[1], args[0], false);
        }
        else if (args.length == 2) {
            KETLBootStrap.startProcess(args[1], args[0], true);
        }
        else if (args.length == 1) {
            KETLBootStrap.startProcess(null, args[0], true);
        }
        else {
            System.out.println("Syntax Error: <Command> {Working directory} {BACKGROUND|FOREGROUND}");
        }
    }

    /**
     * Start process.
     * 
     * @param strWorkingDirectory the str working directory
     * @param pProcessCommand the process command
     * @param pBackground the background
     * 
     * @return true, if successful
     */
    public static boolean startProcess(String strWorkingDirectory, String pProcessCommand, boolean pBackground) {
        Process pProcess = null;
        boolean bSuccess = true;

        // Create a File object to define the working directory (if specified)...
        if (strWorkingDirectory == null) {
            strWorkingDirectory = System.getProperty("user.dir");
        }

        try {
            String osName = System.getProperty("os.name");
            String strExecStmt;

            if (osName.startsWith("Windows")) {
                strExecStmt = "cmd.exe /c " + pProcessCommand;
            }
            else // assume some UNIX/Linux system
            {
                strExecStmt = pProcessCommand;
            }

            File x = new File(strWorkingDirectory);

            pProcess = Runtime.getRuntime().exec(strExecStmt, null, x);
        } catch (Exception e) {
            System.out.println("Error running exec(): " + e.getMessage());

            return false;
        }

        // Wait for the process to finish and return the exit code.
        // BRIAN: we should probably do a periodic call to exitStatus() and catch the exception until the
        // process is done. This way, we can terminate the process during a shutdown.
        try {
            if (pBackground == false) {
                BufferedReader in = new BufferedReader(new InputStreamReader(pProcess.getInputStream()));
                String currentLine = null;

                while ((currentLine = in.readLine()) != null)
                    System.out.println(currentLine);

                BufferedReader err = new BufferedReader(new InputStreamReader(pProcess.getErrorStream()));

                while ((currentLine = err.readLine()) != null)
                    System.out.println(currentLine);

                int iReturnValue = pProcess.waitFor();

                if (iReturnValue != 0) {
                    bSuccess = false;
                }
            }
            else {
                bSuccess = true;
            }
        } catch (Exception e) {
            System.out.println("Error in process: " + e.getMessage());

            return false;
        }

        return bSuccess;
    }
}
