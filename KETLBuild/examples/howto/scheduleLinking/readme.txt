Scenario
User wants to schedule their jobs after the execution of another schedule.

Options
1. If the job metadata is within the same metadata of the parent schedule, use the ExecuteJobWriter.
2. If the job metadata is in a different repository, then you need to have the parent schedule trigger your schedule
   a - Modify the root job of the parent schedule so that it is a SQLJob
   b - Define the sql as 'update job_schedule set next_run_date = sysdate where job_id = 'MyTargetRootJob''
   c - Define the SQLJob datasource as the target metadata
   
   See option2Parent.xml option2Target.xml for an example of what to do