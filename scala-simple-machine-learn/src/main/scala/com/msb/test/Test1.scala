package com.msb.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * repartition改变分区数验证
  */
object Test1 {

  def main(args: Array[String]): Unit = {
    println("111:",this.getClass.getSimpleName)
    val session = SparkSession.builder.appName(this.getClass.getSimpleName).master("local").getOrCreate()
    import session.implicits._
//    val conf = new SparkConf().setAppName("DecisionTreeDemo").setMaster("local[4]")
//    val sc = new SparkContext(conf)
    val data = session.sparkContext.textFile("D:\\data\\spark\\text\\data-1.txt")
    data.collect
    println("abc:",data.partitions.size)
//    data.foreach(println)

    val rdd2 = data.mapPartitionsWithIndex(func1)
    println("edf:", rdd2.partitions.size)
    rdd2.toDF().show(100) //数据显示
//    rdd2.mapPartitionsWithIndex()

    val rdd3 = data.repartition(3).mapPartitionsWithIndex(func1)
    println("ghk:", rdd3.partitions.size)
    rdd3.toDF().show(100)

    session.close()
  }

  /**
    * mapPartitionsWithIndex算子用
    * @param index 分区号
    * @param iter 数据
    * @return
    */
  def func1(index:Int,iter:Iterator[String]) : Iterator[String] = {
    iter.toList.map(x => "[partID:"+index+",value:"+x+"]").iterator
  }

}

//执行日志
/*
"C:\Program Files\Java\jdk1.8.0_144\bin\java.exe" "-javaagent:D:\tools\idea\IntelliJ IDEA 2019.2\lib\idea_rt.jar=54295:D:\tools\idea\IntelliJ IDEA 2019.2\bin" -Dfile.encoding=UTF-8 -classpath "C:\Program Files\Java\jdk1.8.0_144\jre\lib\charsets.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\deploy.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\access-bridge-64.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\cldrdata.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\dnsns.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\jaccess.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\jfxrt.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\localedata.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\nashorn.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\sunec.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\sunjce_provider.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\sunmscapi.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\sunpkcs11.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\ext\zipfs.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\javaws.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\jce.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\jfr.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\jfxswt.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\jsse.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\management-agent.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\plugin.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\resources.jar;C:\Program Files\Java\jdk1.8.0_144\jre\lib\rt.jar;D:\resource\scala-cluster-full\scala-simple-machine-learn\target\classes;D:\opt\scala\scala-2.11.11\lib\scala-actors-2.11.0.jar;D:\opt\scala\scala-2.11.11\lib\scala-actors-migration_2.11-1.1.0.jar;D:\opt\scala\scala-2.11.11\lib\scala-library.jar;D:\opt\scala\scala-2.11.11\lib\scala-parser-combinators_2.11-1.0.4.jar;D:\opt\scala\scala-2.11.11\lib\scala-reflect.jar;D:\opt\scala\scala-2.11.11\lib\scala-swing_2.11-1.0.2.jar;D:\opt\scala\scala-2.11.11\lib\scala-xml_2.11-1.0.5.jar;D:\apache-maven\repository\org\scala-lang\scala-library\2.11.11\scala-library-2.11.11.jar;D:\apache-maven\repository\org\scala-lang\scala-actors\2.11.11\scala-actors-2.11.11.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-client\2.7.5\hadoop-client-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-hdfs\2.7.5\hadoop-hdfs-2.7.5.jar;D:\apache-maven\repository\xerces\xercesImpl\2.9.1\xercesImpl-2.9.1.jar;D:\apache-maven\repository\xml-apis\xml-apis\1.3.04\xml-apis-1.3.04.jar;D:\apache-maven\repository\org\fusesource\leveldbjni\leveldbjni-all\1.8\leveldbjni-all-1.8.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-mapreduce-client-app\2.7.5\hadoop-mapreduce-client-app-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-mapreduce-client-common\2.7.5\hadoop-mapreduce-client-common-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-client\2.7.5\hadoop-yarn-client-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-mapreduce-client-shuffle\2.7.5\hadoop-mapreduce-client-shuffle-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-api\2.7.5\hadoop-yarn-api-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-mapreduce-client-jobclient\2.7.5\hadoop-mapreduce-client-jobclient-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-common\2.7.5\hadoop-common-2.7.5.jar;D:\apache-maven\repository\com\google\guava\guava\11.0.2\guava-11.0.2.jar;D:\apache-maven\repository\commons-cli\commons-cli\1.2\commons-cli-1.2.jar;D:\apache-maven\repository\org\apache\commons\commons-math3\3.1.1\commons-math3-3.1.1.jar;D:\apache-maven\repository\xmlenc\xmlenc\0.52\xmlenc-0.52.jar;D:\apache-maven\repository\commons-httpclient\commons-httpclient\3.1\commons-httpclient-3.1.jar;D:\apache-maven\repository\commons-codec\commons-codec\1.4\commons-codec-1.4.jar;D:\apache-maven\repository\commons-io\commons-io\2.4\commons-io-2.4.jar;D:\apache-maven\repository\commons-net\commons-net\3.1\commons-net-3.1.jar;D:\apache-maven\repository\commons-collections\commons-collections\3.2.2\commons-collections-3.2.2.jar;D:\apache-maven\repository\javax\servlet\servlet-api\2.5\servlet-api-2.5.jar;D:\apache-maven\repository\org\mortbay\jetty\jetty\6.1.26\jetty-6.1.26.jar;D:\apache-maven\repository\org\mortbay\jetty\jetty-util\6.1.26\jetty-util-6.1.26.jar;D:\apache-maven\repository\org\mortbay\jetty\jetty-sslengine\6.1.26\jetty-sslengine-6.1.26.jar;D:\apache-maven\repository\javax\servlet\jsp\jsp-api\2.1\jsp-api-2.1.jar;D:\apache-maven\repository\com\sun\jersey\jersey-core\1.9\jersey-core-1.9.jar;D:\apache-maven\repository\com\sun\jersey\jersey-json\1.9\jersey-json-1.9.jar;D:\apache-maven\repository\org\codehaus\jettison\jettison\1.1\jettison-1.1.jar;D:\apache-maven\repository\com\sun\xml\bind\jaxb-impl\2.2.3-1\jaxb-impl-2.2.3-1.jar;D:\apache-maven\repository\org\codehaus\jackson\jackson-xc\1.8.3\jackson-xc-1.8.3.jar;D:\apache-maven\repository\com\sun\jersey\jersey-server\1.9\jersey-server-1.9.jar;D:\apache-maven\repository\asm\asm\3.1\asm-3.1.jar;D:\apache-maven\repository\commons-logging\commons-logging\1.1.3\commons-logging-1.1.3.jar;D:\apache-maven\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;D:\apache-maven\repository\net\java\dev\jets3t\jets3t\0.9.0\jets3t-0.9.0.jar;D:\apache-maven\repository\org\apache\httpcomponents\httpcore\4.1.2\httpcore-4.1.2.jar;D:\apache-maven\repository\com\jamesmurty\utils\java-xmlbuilder\0.4\java-xmlbuilder-0.4.jar;D:\apache-maven\repository\commons-lang\commons-lang\2.6\commons-lang-2.6.jar;D:\apache-maven\repository\commons-configuration\commons-configuration\1.6\commons-configuration-1.6.jar;D:\apache-maven\repository\commons-digester\commons-digester\1.8\commons-digester-1.8.jar;D:\apache-maven\repository\commons-beanutils\commons-beanutils\1.7.0\commons-beanutils-1.7.0.jar;D:\apache-maven\repository\commons-beanutils\commons-beanutils-core\1.8.0\commons-beanutils-core-1.8.0.jar;D:\apache-maven\repository\org\codehaus\jackson\jackson-core-asl\1.9.13\jackson-core-asl-1.9.13.jar;D:\apache-maven\repository\org\codehaus\jackson\jackson-mapper-asl\1.9.13\jackson-mapper-asl-1.9.13.jar;D:\apache-maven\repository\org\apache\avro\avro\1.7.4\avro-1.7.4.jar;D:\apache-maven\repository\com\thoughtworks\paranamer\paranamer\2.3\paranamer-2.3.jar;D:\apache-maven\repository\com\google\protobuf\protobuf-java\2.5.0\protobuf-java-2.5.0.jar;D:\apache-maven\repository\com\google\code\gson\gson\2.2.4\gson-2.2.4.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-auth\2.7.5\hadoop-auth-2.7.5.jar;D:\apache-maven\repository\org\apache\directory\server\apacheds-kerberos-codec\2.0.0-M15\apacheds-kerberos-codec-2.0.0-M15.jar;D:\apache-maven\repository\org\apache\directory\server\apacheds-i18n\2.0.0-M15\apacheds-i18n-2.0.0-M15.jar;D:\apache-maven\repository\org\apache\directory\api\api-asn1-api\1.0.0-M20\api-asn1-api-1.0.0-M20.jar;D:\apache-maven\repository\org\apache\directory\api\api-util\1.0.0-M20\api-util-1.0.0-M20.jar;D:\apache-maven\repository\org\apache\curator\curator-framework\2.7.1\curator-framework-2.7.1.jar;D:\apache-maven\repository\com\jcraft\jsch\0.1.54\jsch-0.1.54.jar;D:\apache-maven\repository\org\apache\curator\curator-client\2.7.1\curator-client-2.7.1.jar;D:\apache-maven\repository\org\apache\curator\curator-recipes\2.7.1\curator-recipes-2.7.1.jar;D:\apache-maven\repository\com\google\code\findbugs\jsr305\3.0.0\jsr305-3.0.0.jar;D:\apache-maven\repository\org\apache\htrace\htrace-core\3.1.0-incubating\htrace-core-3.1.0-incubating.jar;D:\apache-maven\repository\org\apache\zookeeper\zookeeper\3.4.6\zookeeper-3.4.6.jar;D:\apache-maven\repository\org\apache\commons\commons-compress\1.4.1\commons-compress-1.4.1.jar;D:\apache-maven\repository\org\tukaani\xz\1.0\xz-1.0.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-mapreduce-client-core\2.7.5\hadoop-mapreduce-client-core-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-common\2.7.5\hadoop-yarn-common-2.7.5.jar;D:\apache-maven\repository\javax\xml\bind\jaxb-api\2.2.2\jaxb-api-2.2.2.jar;D:\apache-maven\repository\javax\xml\stream\stax-api\1.0-2\stax-api-1.0-2.jar;D:\apache-maven\repository\javax\activation\activation\1.1\activation-1.1.jar;D:\apache-maven\repository\com\sun\jersey\jersey-client\1.9\jersey-client-1.9.jar;D:\apache-maven\repository\com\google\inject\guice\3.0\guice-3.0.jar;D:\apache-maven\repository\javax\inject\javax.inject\1\javax.inject-1.jar;D:\apache-maven\repository\aopalliance\aopalliance\1.0\aopalliance-1.0.jar;D:\apache-maven\repository\com\sun\jersey\contribs\jersey-guice\1.9\jersey-guice-1.9.jar;D:\apache-maven\repository\com\google\inject\extensions\guice-servlet\3.0\guice-servlet-3.0.jar;D:\apache-maven\repository\io\netty\netty\3.6.2.Final\netty-3.6.2.Final.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-annotations\2.7.5\hadoop-annotations-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-minicluster\2.7.5\hadoop-minicluster-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-common\2.7.5\hadoop-common-2.7.5-tests.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-hdfs\2.7.5\hadoop-hdfs-2.7.5-tests.jar;D:\apache-maven\repository\commons-daemon\commons-daemon\1.0.13\commons-daemon-1.0.13.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-server-tests\2.7.5\hadoop-yarn-server-tests-2.7.5-tests.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-server-common\2.7.5\hadoop-yarn-server-common-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-server-nodemanager\2.7.5\hadoop-yarn-server-nodemanager-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-server-resourcemanager\2.7.5\hadoop-yarn-server-resourcemanager-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-server-applicationhistoryservice\2.7.5\hadoop-yarn-server-applicationhistoryservice-2.7.5.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-yarn-server-web-proxy\2.7.5\hadoop-yarn-server-web-proxy-2.7.5.jar;D:\apache-maven\repository\org\apache\zookeeper\zookeeper\3.4.6\zookeeper-3.4.6-tests.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-mapreduce-client-jobclient\2.7.5\hadoop-mapreduce-client-jobclient-2.7.5-tests.jar;D:\apache-maven\repository\org\apache\hadoop\hadoop-mapreduce-client-hs\2.7.5\hadoop-mapreduce-client-hs-2.7.5.jar;D:\apache-maven\repository\org\apache\spark\spark-core_2.11\2.3.4\spark-core_2.11-2.3.4.jar;D:\apache-maven\repository\org\apache\avro\avro-mapred\1.7.7\avro-mapred-1.7.7-hadoop2.jar;D:\apache-maven\repository\org\apache\avro\avro-ipc\1.7.7\avro-ipc-1.7.7.jar;D:\apache-maven\repository\org\apache\avro\avro-ipc\1.7.7\avro-ipc-1.7.7-tests.jar;D:\apache-maven\repository\com\twitter\chill_2.11\0.8.4\chill_2.11-0.8.4.jar;D:\apache-maven\repository\com\esotericsoftware\kryo-shaded\3.0.3\kryo-shaded-3.0.3.jar;D:\apache-maven\repository\com\esotericsoftware\minlog\1.3.0\minlog-1.3.0.jar;D:\apache-maven\repository\org\objenesis\objenesis\2.1\objenesis-2.1.jar;D:\apache-maven\repository\com\twitter\chill-java\0.8.4\chill-java-0.8.4.jar;D:\apache-maven\repository\org\apache\xbean\xbean-asm5-shaded\4.4\xbean-asm5-shaded-4.4.jar;D:\apache-maven\repository\org\apache\spark\spark-launcher_2.11\2.3.4\spark-launcher_2.11-2.3.4.jar;D:\apache-maven\repository\org\apache\spark\spark-kvstore_2.11\2.3.4\spark-kvstore_2.11-2.3.4.jar;D:\apache-maven\repository\com\fasterxml\jackson\core\jackson-core\2.6.7\jackson-core-2.6.7.jar;D:\apache-maven\repository\com\fasterxml\jackson\core\jackson-annotations\2.6.7\jackson-annotations-2.6.7.jar;D:\apache-maven\repository\org\apache\spark\spark-network-common_2.11\2.3.4\spark-network-common_2.11-2.3.4.jar;D:\apache-maven\repository\org\apache\spark\spark-network-shuffle_2.11\2.3.4\spark-network-shuffle_2.11-2.3.4.jar;D:\apache-maven\repository\org\apache\spark\spark-unsafe_2.11\2.3.4\spark-unsafe_2.11-2.3.4.jar;D:\apache-maven\repository\javax\servlet\javax.servlet-api\3.1.0\javax.servlet-api-3.1.0.jar;D:\apache-maven\repository\org\apache\commons\commons-lang3\3.5\commons-lang3-3.5.jar;D:\apache-maven\repository\org\slf4j\jul-to-slf4j\1.7.16\jul-to-slf4j-1.7.16.jar;D:\apache-maven\repository\org\slf4j\jcl-over-slf4j\1.7.16\jcl-over-slf4j-1.7.16.jar;D:\apache-maven\repository\com\ning\compress-lzf\1.0.3\compress-lzf-1.0.3.jar;D:\apache-maven\repository\org\xerial\snappy\snappy-java\1.1.2.6\snappy-java-1.1.2.6.jar;D:\apache-maven\repository\org\lz4\lz4-java\1.4.0\lz4-java-1.4.0.jar;D:\apache-maven\repository\com\github\luben\zstd-jni\1.3.2-2\zstd-jni-1.3.2-2.jar;D:\apache-maven\repository\org\roaringbitmap\RoaringBitmap\0.7.45\RoaringBitmap-0.7.45.jar;D:\apache-maven\repository\org\roaringbitmap\shims\0.7.45\shims-0.7.45.jar;D:\apache-maven\repository\org\json4s\json4s-jackson_2.11\3.2.11\json4s-jackson_2.11-3.2.11.jar;D:\apache-maven\repository\org\json4s\json4s-core_2.11\3.2.11\json4s-core_2.11-3.2.11.jar;D:\apache-maven\repository\org\json4s\json4s-ast_2.11\3.2.11\json4s-ast_2.11-3.2.11.jar;D:\apache-maven\repository\org\scala-lang\scalap\2.11.0\scalap-2.11.0.jar;D:\apache-maven\repository\org\scala-lang\scala-compiler\2.11.0\scala-compiler-2.11.0.jar;D:\apache-maven\repository\org\scala-lang\modules\scala-xml_2.11\1.0.1\scala-xml_2.11-1.0.1.jar;D:\apache-maven\repository\org\glassfish\jersey\core\jersey-client\2.22.2\jersey-client-2.22.2.jar;D:\apache-maven\repository\javax\ws\rs\javax.ws.rs-api\2.0.1\javax.ws.rs-api-2.0.1.jar;D:\apache-maven\repository\org\glassfish\hk2\hk2-api\2.4.0-b34\hk2-api-2.4.0-b34.jar;D:\apache-maven\repository\org\glassfish\hk2\hk2-utils\2.4.0-b34\hk2-utils-2.4.0-b34.jar;D:\apache-maven\repository\org\glassfish\hk2\external\aopalliance-repackaged\2.4.0-b34\aopalliance-repackaged-2.4.0-b34.jar;D:\apache-maven\repository\org\glassfish\hk2\external\javax.inject\2.4.0-b34\javax.inject-2.4.0-b34.jar;D:\apache-maven\repository\org\glassfish\hk2\hk2-locator\2.4.0-b34\hk2-locator-2.4.0-b34.jar;D:\apache-maven\repository\org\javassist\javassist\3.18.1-GA\javassist-3.18.1-GA.jar;D:\apache-maven\repository\org\glassfish\jersey\core\jersey-common\2.22.2\jersey-common-2.22.2.jar;D:\apache-maven\repository\javax\annotation\javax.annotation-api\1.2\javax.annotation-api-1.2.jar;D:\apache-maven\repository\org\glassfish\jersey\bundles\repackaged\jersey-guava\2.22.2\jersey-guava-2.22.2.jar;D:\apache-maven\repository\org\glassfish\hk2\osgi-resource-locator\1.0.1\osgi-resource-locator-1.0.1.jar;D:\apache-maven\repository\org\glassfish\jersey\core\jersey-server\2.22.2\jersey-server-2.22.2.jar;D:\apache-maven\repository\org\glassfish\jersey\media\jersey-media-jaxb\2.22.2\jersey-media-jaxb-2.22.2.jar;D:\apache-maven\repository\javax\validation\validation-api\1.1.0.Final\validation-api-1.1.0.Final.jar;D:\apache-maven\repository\org\glassfish\jersey\containers\jersey-container-servlet\2.22.2\jersey-container-servlet-2.22.2.jar;D:\apache-maven\repository\org\glassfish\jersey\containers\jersey-container-servlet-core\2.22.2\jersey-container-servlet-core-2.22.2.jar;D:\apache-maven\repository\io\netty\netty-all\4.1.17.Final\netty-all-4.1.17.Final.jar;D:\apache-maven\repository\com\clearspring\analytics\stream\2.7.0\stream-2.7.0.jar;D:\apache-maven\repository\io\dropwizard\metrics\metrics-core\3.1.5\metrics-core-3.1.5.jar;D:\apache-maven\repository\io\dropwizard\metrics\metrics-jvm\3.1.5\metrics-jvm-3.1.5.jar;D:\apache-maven\repository\io\dropwizard\metrics\metrics-json\3.1.5\metrics-json-3.1.5.jar;D:\apache-maven\repository\io\dropwizard\metrics\metrics-graphite\3.1.5\metrics-graphite-3.1.5.jar;D:\apache-maven\repository\com\fasterxml\jackson\core\jackson-databind\2.6.7.1\jackson-databind-2.6.7.1.jar;D:\apache-maven\repository\com\fasterxml\jackson\module\jackson-module-scala_2.11\2.6.7.1\jackson-module-scala_2.11-2.6.7.1.jar;D:\apache-maven\repository\com\fasterxml\jackson\module\jackson-module-paranamer\2.7.9\jackson-module-paranamer-2.7.9.jar;D:\apache-maven\repository\org\apache\ivy\ivy\2.4.0\ivy-2.4.0.jar;D:\apache-maven\repository\oro\oro\2.0.8\oro-2.0.8.jar;D:\apache-maven\repository\net\razorvine\pyrolite\4.13\pyrolite-4.13.jar;D:\apache-maven\repository\net\sf\py4j\py4j\0.10.7\py4j-0.10.7.jar;D:\apache-maven\repository\org\apache\spark\spark-tags_2.11\2.3.4\spark-tags_2.11-2.3.4.jar;D:\apache-maven\repository\org\apache\commons\commons-crypto\1.0.0\commons-crypto-1.0.0.jar;D:\apache-maven\repository\org\spark-project\spark\unused\1.0.0\unused-1.0.0.jar;D:\apache-maven\repository\org\apache\spark\spark-mllib_2.11\2.3.4\spark-mllib_2.11-2.3.4.jar;D:\apache-maven\repository\org\scala-lang\modules\scala-parser-combinators_2.11\1.0.4\scala-parser-combinators_2.11-1.0.4.jar;D:\apache-maven\repository\org\apache\spark\spark-streaming_2.11\2.3.4\spark-streaming_2.11-2.3.4.jar;D:\apache-maven\repository\org\apache\spark\spark-sql_2.11\2.3.4\spark-sql_2.11-2.3.4.jar;D:\apache-maven\repository\com\univocity\univocity-parsers\2.5.9\univocity-parsers-2.5.9.jar;D:\apache-maven\repository\org\apache\spark\spark-sketch_2.11\2.3.4\spark-sketch_2.11-2.3.4.jar;D:\apache-maven\repository\org\apache\spark\spark-catalyst_2.11\2.3.4\spark-catalyst_2.11-2.3.4.jar;D:\apache-maven\repository\org\codehaus\janino\janino\3.0.8\janino-3.0.8.jar;D:\apache-maven\repository\org\codehaus\janino\commons-compiler\3.0.8\commons-compiler-3.0.8.jar;D:\apache-maven\repository\org\antlr\antlr4-runtime\4.7\antlr4-runtime-4.7.jar;D:\apache-maven\repository\org\apache\orc\orc-core\1.4.4\orc-core-1.4.4-nohive.jar;D:\apache-maven\repository\io\airlift\aircompressor\0.8\aircompressor-0.8.jar;D:\apache-maven\repository\org\apache\orc\orc-mapreduce\1.4.4\orc-mapreduce-1.4.4-nohive.jar;D:\apache-maven\repository\org\apache\parquet\parquet-column\1.8.3\parquet-column-1.8.3.jar;D:\apache-maven\repository\org\apache\parquet\parquet-common\1.8.3\parquet-common-1.8.3.jar;D:\apache-maven\repository\org\apache\parquet\parquet-encoding\1.8.3\parquet-encoding-1.8.3.jar;D:\apache-maven\repository\org\apache\parquet\parquet-hadoop\1.8.3\parquet-hadoop-1.8.3.jar;D:\apache-maven\repository\org\apache\parquet\parquet-format\2.3.1\parquet-format-2.3.1.jar;D:\apache-maven\repository\org\apache\parquet\parquet-jackson\1.8.3\parquet-jackson-1.8.3.jar;D:\apache-maven\repository\org\apache\arrow\arrow-vector\0.8.0\arrow-vector-0.8.0.jar;D:\apache-maven\repository\org\apache\arrow\arrow-format\0.8.0\arrow-format-0.8.0.jar;D:\apache-maven\repository\org\apache\arrow\arrow-memory\0.8.0\arrow-memory-0.8.0.jar;D:\apache-maven\repository\com\carrotsearch\hppc\0.7.2\hppc-0.7.2.jar;D:\apache-maven\repository\com\vlkan\flatbuffers\1.2.0-3f79e055\flatbuffers-1.2.0-3f79e055.jar;D:\apache-maven\repository\org\apache\spark\spark-graphx_2.11\2.3.4\spark-graphx_2.11-2.3.4.jar;D:\apache-maven\repository\com\github\fommil\netlib\core\1.1.2\core-1.1.2.jar;D:\apache-maven\repository\net\sourceforge\f2j\arpack_combined_all\0.1\arpack_combined_all-0.1.jar;D:\apache-maven\repository\org\apache\spark\spark-mllib-local_2.11\2.3.4\spark-mllib-local_2.11-2.3.4.jar;D:\apache-maven\repository\org\scalanlp\breeze_2.11\0.13.2\breeze_2.11-0.13.2.jar;D:\apache-maven\repository\org\scalanlp\breeze-macros_2.11\0.13.2\breeze-macros_2.11-0.13.2.jar;D:\apache-maven\repository\net\sf\opencsv\opencsv\2.3\opencsv-2.3.jar;D:\apache-maven\repository\com\github\rwl\jtransforms\2.4.0\jtransforms-2.4.0.jar;D:\apache-maven\repository\org\spire-math\spire_2.11\0.13.0\spire_2.11-0.13.0.jar;D:\apache-maven\repository\org\spire-math\spire-macros_2.11\0.13.0\spire-macros_2.11-0.13.0.jar;D:\apache-maven\repository\org\typelevel\machinist_2.11\0.6.1\machinist_2.11-0.6.1.jar;D:\apache-maven\repository\com\chuusai\shapeless_2.11\2.3.2\shapeless_2.11-2.3.2.jar;D:\apache-maven\repository\org\typelevel\macro-compat_2.11\1.1.1\macro-compat_2.11-1.1.1.jar;D:\apache-maven\repository\org\apache\httpcomponents\httpclient\4.5.4\httpclient-4.5.4.jar;D:\apache-maven\repository\joda-time\joda-time\2.9.3\joda-time-2.9.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-client\1.3.3\hbase-client-1.3.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-annotations\1.3.3\hbase-annotations-1.3.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-protocol\1.3.3\hbase-protocol-1.3.3.jar;D:\apache-maven\repository\org\jruby\jcodings\jcodings\1.0.8\jcodings-1.0.8.jar;D:\apache-maven\repository\org\jruby\joni\joni\2.1.2\joni-2.1.2.jar;D:\apache-maven\repository\com\yammer\metrics\metrics-core\2.2.0\metrics-core-2.2.0.jar;D:\apache-maven\repository\org\apache\hbase\hbase-common\1.3.3\hbase-common-1.3.3.jar;D:\apache-maven\repository\com\github\stephenc\findbugs\findbugs-annotations\1.3.9-1\findbugs-annotations-1.3.9-1.jar;D:\apache-maven\repository\junit\junit\4.12\junit-4.12.jar;D:\apache-maven\repository\org\hamcrest\hamcrest-core\1.3\hamcrest-core-1.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-server\1.3.3\hbase-server-1.3.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-procedure\1.3.3\hbase-procedure-1.3.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-common\1.3.3\hbase-common-1.3.3-tests.jar;D:\apache-maven\repository\org\apache\hbase\hbase-prefix-tree\1.3.3\hbase-prefix-tree-1.3.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-hadoop-compat\1.3.3\hbase-hadoop-compat-1.3.3.jar;D:\apache-maven\repository\org\apache\hbase\hbase-hadoop2-compat\1.3.3\hbase-hadoop2-compat-1.3.3.jar;D:\apache-maven\repository\org\apache\commons\commons-math\2.2\commons-math-2.2.jar;D:\apache-maven\repository\org\mortbay\jetty\jsp-2.1\6.1.14\jsp-2.1-6.1.14.jar;D:\apache-maven\repository\org\mortbay\jetty\jsp-api-2.1\6.1.14\jsp-api-2.1-6.1.14.jar;D:\apache-maven\repository\org\mortbay\jetty\servlet-api-2.5\6.1.14\servlet-api-2.5-6.1.14.jar;D:\apache-maven\repository\org\codehaus\jackson\jackson-jaxrs\1.9.13\jackson-jaxrs-1.9.13.jar;D:\apache-maven\repository\tomcat\jasper-compiler\5.5.23\jasper-compiler-5.5.23.jar;D:\apache-maven\repository\tomcat\jasper-runtime\5.5.23\jasper-runtime-5.5.23.jar;D:\apache-maven\repository\commons-el\commons-el\1.0\commons-el-1.0.jar;D:\apache-maven\repository\org\jamon\jamon-runtime\2.4.1\jamon-runtime-2.4.1.jar;D:\apache-maven\repository\com\lmax\disruptor\3.3.0\disruptor-3.3.0.jar;D:\apache-maven\repository\com\typesafe\akka\akka-actor_2.11\2.5.25\akka-actor_2.11-2.5.25.jar;D:\apache-maven\repository\com\typesafe\config\1.3.3\config-1.3.3.jar;D:\apache-maven\repository\org\scala-lang\modules\scala-java8-compat_2.11\0.7.0\scala-java8-compat_2.11-0.7.0.jar;D:\apache-maven\repository\com\typesafe\akka\akka-slf4j_2.11\2.5.25\akka-slf4j_2.11-2.5.25.jar;D:\apache-maven\repository\com\typesafe\akka\akka-stream_2.11\2.5.25\akka-stream_2.11-2.5.25.jar;D:\apache-maven\repository\com\typesafe\akka\akka-protobuf_2.11\2.5.25\akka-protobuf_2.11-2.5.25.jar;D:\apache-maven\repository\org\reactivestreams\reactive-streams\1.0.2\reactive-streams-1.0.2.jar;D:\apache-maven\repository\com\typesafe\ssl-config-core_2.11\0.3.8\ssl-config-core_2.11-0.3.8.jar;D:\apache-maven\repository\com\typesafe\akka\akka-remote_2.11\2.5.25\akka-remote_2.11-2.5.25.jar;D:\apache-maven\repository\io\aeron\aeron-driver\1.15.1\aeron-driver-1.15.1.jar;D:\apache-maven\repository\org\agrona\Agrona\0.9.31\agrona-0.9.31.jar;D:\apache-maven\repository\io\aeron\aeron-client\1.15.1\aeron-client-1.15.1.jar;D:\apache-maven\repository\com\typesafe\akka\akka-cluster_2.11\2.5.25\akka-cluster_2.11-2.5.25.jar;D:\apache-maven\repository\org\apache\kafka\kafka_2.11\1.1.1\kafka_2.11-1.1.1.jar;D:\apache-maven\repository\org\apache\kafka\kafka-clients\1.1.1\kafka-clients-1.1.1.jar;D:\apache-maven\repository\net\sf\jopt-simple\jopt-simple\5.0.4\jopt-simple-5.0.4.jar;D:\apache-maven\repository\org\scala-lang\scala-reflect\2.11.12\scala-reflect-2.11.12.jar;D:\apache-maven\repository\com\typesafe\scala-logging\scala-logging_2.11\3.8.0\scala-logging_2.11-3.8.0.jar;D:\apache-maven\repository\com\101tec\zkclient\0.10\zkclient-0.10.jar;D:\apache-maven\repository\org\slf4j\slf4j-api\1.7.25\slf4j-api-1.7.25.jar;D:\apache-maven\repository\org\slf4j\slf4j-nop\1.7.25\slf4j-nop-1.7.25.jar;D:\apache-maven\repository\com\janeluo\ikanalyzer\2012_u6\ikanalyzer-2012_u6.jar;D:\apache-maven\repository\org\apache\lucene\lucene-core\4.7.2\lucene-core-4.7.2.jar;D:\apache-maven\repository\org\apache\lucene\lucene-queryparser\4.7.2\lucene-queryparser-4.7.2.jar;D:\apache-maven\repository\org\apache\lucene\lucene-queries\4.7.2\lucene-queries-4.7.2.jar;D:\apache-maven\repository\org\apache\lucene\lucene-sandbox\4.7.2\lucene-sandbox-4.7.2.jar;D:\apache-maven\repository\org\apache\lucene\lucene-analyzers-common\4.7.2\lucene-analyzers-common-4.7.2.jar;D:\apache-maven\repository\com\alibaba\fastjson\1.2.52\fastjson-1.2.52.jar" com.msb.test.Test1
(111:,Test1$)
(abc:,1)
(edf:,1)
+--------------------+
|               value|
+--------------------+
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
+--------------------+

(ghk:,3)
+--------------------+
|               value|
+--------------------+
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
+--------------------+


Process finished with exit code 0


  */