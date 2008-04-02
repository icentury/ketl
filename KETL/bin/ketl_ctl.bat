echo off
set CURRENT_DIR=%CD%

if not defined KETLDIR goto ERROR

cd "%KETLDIR%"

"%JAVA_HOME%\bin\java" %JAVA_OPTS% "-Dlog4j.configuration=file:%KETLDIR%\conf\Console.log.properties"  -classpath "%JAVA_HOME%\lib\tools.jar;%KETLDIR%\lib\KETL.jar"  Console
cd "%CURRENT_DIR%"
GOTO END

:ERROR
echo "KETLDIR needs to be set"
:END


