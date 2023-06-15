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
parser.add_argument('--vcampoparte', required=False, type=str, help='Campo de particion, este campo debe ser definido si es una carga tipo append')
parser.add_argument('--vfechai', required=False, type=str,help='Parametro 1 de la query sql')
parser.add_argument('--vfechaf', required=False, type=str,help='Parametro 1 de la query sql')

parametros = parser.parse_args()
vClass=parametros.vclass
vUrlJdbc=parametros.vjdbcurl
vUsuarioBD=parametros.vusuariobd
vClaveBD=parametros.vclavebd
vBaseHive=parametros.vhivebd
vTablaHive=parametros.vtablahive
vTipoCarga=parametros.vtipocarga
vCampoParte=parametros.vcampoparte
vfechai=parametros.vfechai
vfechaf=parametros.vfechaf


vSQL="""
SELECT 
office_code, 
office_name, 
usuario, 
account_num, 
num_abonado, 
bill_dtm, 
document_type_id, 
documento, 
origin_doc_type_id, 
document_type_name, 
nro_nota_credito, 
origin_invoice_num, 
customer_id_type, 
customer_id_number, 
nombre_cliente, 
revenue_code_id, 
revenue_code_desc, 
imei_imsi, 
monto,
TO_CHAR(bill_dtm,'yyyyMMdd') AS pt_fecha
FROM rbm_reportes.otc_t_terminales_nc
WHERE BILL_DTM >= to_date('{vfechai}','yyyyMMdd') AND BILL_DTM < to_date('{vfechaf}','yyyyMMdd')
""".format(vfechai=vfechai, vfechaf=vfechaf)
print(vSQL)

## 2.- Inicio el SparkSession
spark = SparkSession. \
    builder. \
    enableHiveSupport(). \
    config("hive.exec.dynamic.partition", "true"). \
    config("hive.exec.dynamic.partition.mode", "nonstrict"). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
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
    print(etq_info(log_p_parametros("vCampoParte",str(vCampoParte))))
    print(etq_info(log_p_parametros("vfechai",str(vfechai))))
    print(etq_info(log_p_parametros("vfechaf",str(vfechaf))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp00,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp00,str(e))))


print(lne_dvs())
vStp01='PASO [1]: Conexion a la base de datos'
try:
    ts_step = datetime.now()  
    print(etq_info(str(vStp01)))
    print(etq_sql(vSQL))
    df0 = spark.read.format("jdbc")\
			.option("url",vUrlJdbc)\
			.option("driver",vClass)\
			.option("user",vUsuarioBD)\
			.option("password",vClaveBD)\
			.option("fetchsize",1000)\
            .option("dbtable","({})".format(vSQL))\
			.load()
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        df1 = df0.withColumn("fechacarga",F.lit(datetime.now())).withColumn("pt_fecha", F.col('pt_fecha').cast("int"))
        df1 = df1.select([F.col(x).alias(x.lower()) for x in df1.columns])
        df1.printSchema()
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp01,str(e))))
print(lne_dvs())


print("***************************TABLA A CARGAR****************************")
vStp02='PASO [2]: Escritura en Hive'
try:
    ts_step = datetime.now()
    nme_table=vBaseHive+'.'+vTablaHive
    
    if spark._jsparkSession.catalog().tableExists(vBaseHive, vTablaHive):
        columns = spark.table(nme_table).columns
        cols = []
        for column in columns:
            cols.append(column)
        df3=df1.select(cols)
        df3.printSchema()
        df3.repartition(1).write.format("parquet").partitionBy(vCampoParte).mode(vTipoCarga).saveAsTable("{}.{}".format(vBaseHive,vTablaHive))
    else:
        df3=df1
        df3.printSchema()
        df3.write.option("compression","none").format("orc").partitionBy(vCampoParte).mode(vTipoCarga).saveAsTable("{}.{}".format(vBaseHive,vTablaHive))
    e_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_insert_hive(nme_table,str(e))))  

## 4.- Cierre
spark.stop()
timeend = datetime.now()
duracion = timeend-timestart 
print(etq_info(msg_d_duracion_ejecucion('otc_t_terminales_nc.py',vle_duracion(timestart,timeend))))
print(lne_dvs())

