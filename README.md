# whale-flink
apache flink预研


1 基于DataStream API实现欺诈检测
io.github.homepy.whale.flink.frauddetect package
https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/try-flink/datastream/


mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion=1.14.3 \
    -DgroupId=io.github.homepy.whale-flink \
    -DartifactId=whale-flink \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
    
2 基于 Table API 实现实时报表
io.github.homepy.whale.flink.spendreport package
https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/try-flink/table_api/
