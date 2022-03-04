@SETLOCAL
set BINDIR=%~dp0
cd %BINDIR:~0,-4%
set JAVA_HOME=.\java\windows\jdk-11.0.13
echo JAVA_HOME=%JAVA_HOME%
%JAVA_HOME%\bin\java -classpath .\config\extract.properties;.\lib\dataExtraction-2.0.2-SNAPSHOT-jar-with-dependencies.jar;.\target\dataExtraction-2.0.2-SNAPSHOT-jar-with-dependencies.jar com.guidewire.tools.benchmarking.DataExtractionRunner -query %*