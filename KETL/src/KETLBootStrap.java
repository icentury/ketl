/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;


/*
 * Created on Apr 7, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class KETLBootStrap
{
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        // Start up a KETL server in the background
        if ((args.length == 3) && args[2].equalsIgnoreCase("FOREGROUND"))
        {
            startProcess(args[1], args[0], false);
        }
        else if (args.length == 2)
        {
            startProcess(args[1], args[0], true);
        }
        else if (args.length == 1)
        {
            startProcess(null, args[0], true);
        }
        else
        {
            System.out.println("Syntax Error: <Command> {Working directory} {BACKGROUND|FOREGROUND}");
        }
    }

    public static boolean startProcess(String strWorkingDirectory, String pProcessCommand, boolean pBackground)
    {
        Process pProcess = null;
        boolean bSuccess = true;

        // Create a File object to define the working directory (if specified)...
        if (strWorkingDirectory == null)
        {
            strWorkingDirectory = System.getProperty("user.dir");
        }

        try
        {
            String osName = System.getProperty("os.name");
            String strExecStmt;

            if (osName.startsWith("Windows"))
            {
                strExecStmt = "cmd.exe /c " + pProcessCommand;
            }
            else // assume some UNIX/Linux system
            {
                strExecStmt = pProcessCommand;
            }

            File x = new File(strWorkingDirectory);

            pProcess = Runtime.getRuntime().exec(strExecStmt, null, x);
        }
        catch (Exception e)
        {
            System.out.println("Error running exec(): " + e.getMessage());

            return false;
        }

        // Wait for the process to finish and return the exit code.
        // BRIAN: we should probably do a periodic call to exitStatus() and catch the exception until the
        // process is done.  This way, we can terminate the process during a shutdown.
        try
        {
            if (pBackground == false)
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(pProcess.getInputStream()));
                String currentLine = null;

                while ((currentLine = in.readLine()) != null)
                    System.out.println(currentLine);

                BufferedReader err = new BufferedReader(new InputStreamReader(pProcess.getErrorStream()));

                while ((currentLine = err.readLine()) != null)
                    System.out.println(currentLine);

                int iReturnValue = pProcess.waitFor();

                if (iReturnValue != 0)
                {
                    bSuccess = false;
                }
            }
            else
            {
                bSuccess = true;
            }
        }
        catch (Exception e)
        {
            System.out.println("Error in process: " + e.getMessage());

            return false;
        }

        return bSuccess;
    }
}
