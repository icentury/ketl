echo off
set CURRENT_DIR=%CD%

if not defined KETLDIR goto ERROR

cd "%KETLDIR%"

echo "%JAVA_HOME%\bin\java" %JAVA_OPTS% "-Dlog4j.configuration=file:%KETLDIR%\conf\KETL.log.properties"  -classpath "%JAVA_HOME%\lib\tools.jar;%KETLDIR%\lib\KETL.jar"  RunJob FILE=%CURRENT_DIR%\%1%
cd "%CURRENT_DIR%"
GOTO END

:ERROR
echo "KETLDIR needs to be set"
:END


