@set SCRIPT_HOME=%~dp0
@java -cp "%SCRIPT_HOME%jars/*" -Dfile.encoding=UTF-8 groovy.ui.GroovyMain "%SCRIPT_HOME%bin/DatabaseDiffScript.groovy" -c "%SCRIPT_HOME%Config.groovy" %*
