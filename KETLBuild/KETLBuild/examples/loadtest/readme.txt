Creates a project called "LOAD_TEST"
Add 1,000 SQL jobs to the project which load into 3 different db's
		- Oracle
		- MySQL
		- Postgresql
		
Each db has the same table structure
CREATE TABLE LOAD_TEST_DATA(value text,insert_dt timestamp,update_dt timestamp)

200 Jobs for each db insert 1 incrementing value into the table, such that the data should look like

VALUE   INSERT_DT 			UPDATE_DT
1       12-05-07 12:00am    <Will be greater than max insert date>
2       12-05-07 12:01am
3       12-05-07 12:02am

Value 1 insert and update date should always be equal to or less than any higher value


Example job xml
      <JOB ID="PG_1" PARAMETER_LIST="PG_CONNECTION" NAME="PG_1" PROJECT="LOAD_TEST" TYPE="SQL">
		<SQL>
		<STATEMENT>select 1</STATEMENT>
		</SQL>
      </JOB>