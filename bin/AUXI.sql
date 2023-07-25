pyspark --master yarn --name READTBLSTRNS001 \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.kryo.registrator=com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator \
--conf spark.sql.extensions=com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.datasource.hive.warehouse.read.mode=DIRECT_READER_V2 \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--conf spark.hadoop.hive.metastore.uris="thrift://quisrvbigdata1.otecel.com.ec:9083,thrift://quisrvbigdata10.otecel.com.ec:9083" \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--conf spark.port.maxRetries=100 \

from pyspark_llap import HiveWarehouseSession

spark = SparkSession. \
        builder. \
        enableHiveSupport(). \
        getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
app_id = spark._sc.applicationId
print(app_id)
hive_hwc = HiveWarehouseSession.session(spark).build()

vSql='''SELECT substr(fecha_archivo,1,6) AS i_mes,
count(1) AS i_count
FROM dstreaming.otc_ep_record_generator_udrs
WHERE fecha_archivo>=20220101 
AND fecha_archivo<=20230731
GROUP BY substr(fecha_archivo,1,6)'''

df=spark.sql(vSql).show()