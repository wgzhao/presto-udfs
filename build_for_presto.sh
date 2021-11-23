#!/bin/bash

# build for Trino version 348 or earlier (formly known as PrestoSQL)

os=$(uname -s)
if [ "$os" = "Darwin" ];then
    # macos built-in sed has different behavior with -i option
    sed -i '' 's#<trino.version>358</trino.version>#<presto.version>348</presto.version>#g;s#<groupId>io.trino</groupId>#<groupId>io.prestosql</groupId>#g;s#trino-spi#presto-spi#g;s#trino-main#presto-main#g;s#${trino.version}#${presto.version}#g' pom.xml
   find ./src -name "*.java" | xargs sed -i '' 's#io.trino#io.prestosql#g'
else
    sed -i 's#<trino.version>358</trino.version>#<presto.version>348</presto.version>#g;s#<groupId>io.trino</groupId>#<groupId>io.prestosql</groupId>#g;s#trino-spi#presto-spi#g;s#trino-main#presto-main#g;s#${trino.version}#${presto.version}#g' pom.xml
    find ./src -name "*.java" | xargs sed -i 's#io.trino#io.prestosql#g'
fi
# change the trino package name and version

# change import package name

# change service package name
mv src/main/resources/META-INF/services/io.trino.spi.Plugin src/main/resources/META-INF/services/io.prestosql.spi.Plugin
# build
mvn clean package assembly:single -Dmaven.test.skip=true

# get current version
version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

# copy artifacts to target directory
cp target/udfs-${version}-release.zip udfs-${version}-presto-348.zip
mv src/main/resources/META-INF/services/io.prestosql.spi.Plugin src/main/resources/META-INF/services/io.trino.spi.Plugin
# reset all changes
git reset --hard

