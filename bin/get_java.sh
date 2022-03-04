#!/bin/bash
cd "$(dirname ${BASH_SOURCE[0]})"
cd ..

if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    DEC_JAVA="$JAVA_HOME/bin/java"
elif type -p java; then
    DEC_JAVA=java
else
    echo "JAVA_HOME must be set to a Java 11 JDK"
    exit 1
fi

"$DEC_JAVA" -version

version=$("$DEC_JAVA" -version 2>&1 | awk -F '[".]' '/version/ {print $2}')
if [[ "$version" != "11" ]]; then
    echo "JAVA_HOME must be set to a Java 11 JDK"
    exit 1
fi

