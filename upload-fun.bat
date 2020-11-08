@echo off
call j:\bat\aws\aws_vars.bat
set zipFile=%~dp0\CalcHash.zip
del %zipFile%
pushd %~dp0%\out\production\classes
rem pushd %~dp0%\build\classes\java\main
call rar.bat a -r %zipFile%
rem call rar.bat a -aplib -ep %zipFile% C:\Users\Jarek\.gradle\caches\modules-2\files-2.1\com.amazonaws\aws-java-sdk-s3\1.11.578\93777f92b4a39f3642b24198c9f8c172be9a31f2\aws-java-sdk-s3-1.11.578.jar
rem call rar.bat a -aplib -ep %zipFile% C:\Users\Jarek\.gradle\caches\modules-2\files-2.1\com.amazonaws\aws-java-sdk-core\1.11.578\5ec195529be6dd580eea1e667a9a6ea4c5c91d76\aws-java-sdk-core-1.11.578.jar
popd
pushd %~dp0\src\main\java
call rar.bat a -r %zipFile%
popd
@echo on
"%aws%" lambda update-function-code --function-name calcHash --zip-file fileb://CalcHash.zip
