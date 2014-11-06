#!/bin/sh
mvn package -D skipTests -D maven.javadoc.skip=true && echo '' && cp target/kundera-azure-table-2.14.jar ../kundera-test/lib/  && echo '.jar copied to lib/' && cp target/kundera-azure-table-2.14.jar ../kundera-test/war/WEB-INF/lib/ && echo '.jar copied to war/WEB-INF/lib/'
