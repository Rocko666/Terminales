# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import argparse
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

## 1.-Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser = argparse.ArgumentParser()
parser.add_argument('--vclass', required=True, type=str, help='Clase java del conector JDBC')
parser.add_argument('--vjdbcurl', required=True, type=str, help='URL del conector JDBC Ejm: jdbc:mysql://localhost:3306/base')
parser.add_argument('--vusuariobd', required=True, type=str, help='Usuario de la base de datos')
parser.add_argument('--vclavebd', required=True, type=str, help='Clave de la base de datos')
parser.add_argument('--vhivebd', required=True, type=str, help='Nombre de la base de datos hive (tabla de salida)')
parser.add_argument('--vtablahive', required=True, type=str, help='Nombre de la tabla en hive bd.tabla')
parser.add_argument('--vtipocarga', required=True, type=str, help='Tipo de carga overwrite/append - carga completa/incremental')
parser.add_argument('--vfechai', required=False, type=str,help='Parametro 1 de la query sql')
parser.add_argument('--vfechaf', required=False, type=str,help='Parametro 1 de la query sql')

vTTemp="db_desarrollo2021.tmp_otc_t_r_am_cpe"

parametros = parser.parse_args()
vClass=parametros.vclass
vUrlJdbc=parametros.vjdbcurl
vUsuarioBD=parametros.vusuariobd
vClaveBD=parametros.vclavebd
vBaseHive=parametros.vhivebd
vTablaHive=parametros.vtablahive
vTipoCarga=parametros.vtipocarga
vfechai=parametros.vfechai
vfechaf=parametros.vfechaf
nme_table=vBaseHive+'.'+vTablaHive  ##db_desarrollo2021.otc_t_r_am_cpe
fecha_ini=datetime.strptime(vfechai, '%Y%m%d').date()
fecha_fin=datetime.strptime(vfechaf, '%Y%m%d').date()

vSQL_ORA="""
SELECT actual_property, 
associated_iccid, 
consignation_date, 
created_by, 
created_when, 
customer_account, 
description, 
equipment_condition, 
for_temporary_replacement, 
imei, 
is_kitted, 
logical_status, 
modified_by, 
modified_when, 
name, 
number_of_repairs, 
object_id, 
parent_id, 
physical_status, 
project_id, 
purchase_price, 
source_system, 
stock_item_model, 
tfn_warranty_date, 
ticket_number, 
type_id, 
vendor_warranty_date 
FROM proxtomsrep_rdb.r_am_cpe
WHERE created_when >= to_date('{vfechai}','yyyyMMdd') AND created_when < to_date('{vfechaf}','yyyyMMdd')
""".format(vfechai=vfechai, vfechaf=vfechaf)
print(vSQL_ORA)

vSql_Hive="""
SELECT a.actual_property,
a.associated_iccid,
a.consignation_date,
a.created_by,
a.created_when,
a.customer_account,
a.description,
a.equipment_condition,
a.for_temporary_replacement,
a.imei,
a.is_kitted,
a.logical_status,
a.modified_by,
a.modified_when,
a.name,
a.number_of_repairs,
a.object_id,
a.parent_id,
a.physical_status,
a.project_id,
a.purchase_price,
a.source_system,
a.stock_item_model,
a.tfn_warranty_date,
a.ticket_number,
a.type_id,
a.vendor_warranty_date
FROM {nme_table} a
LEFT OUTER JOIN (SELECT b.created_when FROM {nme_table} b WHERE created_when>='{fecha_ini}' AND created_when<'{fecha_fin}') b
ON a.created_when = b.created_when
WHERE b.created_when IS NULL
""".format(nme_table=nme_table,fecha_ini=fecha_ini,fecha_fin=fecha_fin)


vSql_HiveTmp="""
SELECT a.actual_property,
a.associated_iccid,
a.consignation_date,
a.created_by,
a.created_when,
a.customer_account,
a.description,
a.equipment_condition,
a.for_temporary_replacement,
a.imei,
a.is_kitted,
a.logical_status,
a.modified_by,
a.modified_when,
a.name,
a.number_of_repairs,
a.object_id,
a.parent_id,
a.physical_status,
a.project_id,
a.purchase_price,
a.source_system,
a.stock_item_model,
a.tfn_warranty_date,
a.ticket_number,
a.type_id,
a.vendor_warranty_date
FROM {vTTemp} a
""".format(vTTemp=vTTemp)


## 2.- Inicio el SparkSession
spark = SparkSession. \
    builder. \
    enableHiveSupport() \
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.yarn.queue", "default") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId

timestart = datetime.now()
print(lne_dvs())
vStp00='Paso [0]: Iniciando proceso/Cargando configuracion..'

try:
    ts_step = datetime.now()
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vUrlJdbc",str(vUrlJdbc))))
    print(etq_info(log_p_parametros("vClass",str(vClass))))
    print(etq_info(log_p_parametros("vUsuarioBD",str(vUsuarioBD))))
    print(etq_info(log_p_parametros("vClaveBD",str(vClaveBD))))
    print(etq_info(log_p_parametros("vBaseHive",str(vBaseHive))))
    print(etq_info(log_p_parametros("vTablaHive",str(vTablaHive))))
    print(etq_info(log_p_parametros("vTipoCarga",str(vTipoCarga))))
    print(etq_info(log_p_parametros("vfechai",str(vfechai))))
    print(etq_info(log_p_parametros("vfechaf",str(vfechaf))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp00,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp00,str(e))))


print(lne_dvs())
vStp01='PASO [1]: Conexion a la base de datos y escritura '
try:
    ts_step = datetime.now()  
    print(etq_info(str(vStp01)))
    print(etq_sql(vSQL_ORA))
    df0 = spark.read.format("jdbc")\
			.option("url",vUrlJdbc)\
			.option("driver",vClass)\
			.option("user",vUsuarioBD)\
			.option("password",vClaveBD)\
			.option("fetchsize",1000)\
            .option("dbtable","({})".format(vSQL_ORA))\
			.load()
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        #df1 = df0.withColumn("fechacarga",F.lit(datetime.now()))
        df1 = df0
        df1 = df1.select([F.col(x).alias(x.lower()) for x in df1.columns])
        df1.printSchema()
        df1.write.format('parquet').mode("overwrite").saveAsTable(vTTemp)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp01,str(e))))
print(lne_dvs())

print("***************************TABLA A CARGAR****************************")
vStp02='PASO [2]: Borrado e insercion de datos Hive'
try:
    print(etq_info(str(vStp02)))
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(nme_table)))
    ts_step = datetime.now()   
    df_hive = spark.sql(vSql_Hive)
    print(vSql_Hive)
    print('tabla union:')
    df_insert = df1.union(df_hive)
    df_insert.write.format('parquet').mode('overwrite').saveAsTable(vTTemp)
    df_fin = spark.sql(vSql_HiveTmp)
    columns = spark.table(nme_table).columns
    cols = []
    for column in columns:
        cols.append(column)
    df3=df_fin.select(cols)
    df3.printSchema()
    print('tabla final:')
    df3.write.format("parquet").mode(vTipoCarga).saveAsTable("{}.{}".format(vBaseHive,vTablaHive))
    print(etq_info(msg_t_total_registros_hive(nme_table,str(df3.count()))))
    print(lne_dvs())
    #spark.sql(drop_tmp(vEsquema_tmp,vTablaTmp2))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_insert_hive(nme_table,str(e))))  


## 4.- Cierre
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion('otc_t_r_am_cpe.py',vle_duracion(timestart,timeend))))
print(lne_dvs())

