@echo off
set classpath=%~dp0\build\libs\aws.jar
call setClasspath.bat
@echo on
java example.Upload J:\lang\java\aws\go.bat temp1.bat
