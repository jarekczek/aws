@echo off
call j:\bat\aws\aws_vars.bat
"%aws%" lambda invoke --function-name calcHash --payload fileb://in.json out.txt
type out.txt
