# -- coding: utf-8 --
import sys
reload(sys)
from query import *
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

## STEP 1: Definir variables o constantes
vLogInfo='INFO:'
vLogError='ERROR:'

timestart = datetime.now()
## STEP 2: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=False, type=str,help='Parametro de la entidad')
parser.add_argument('--vhivebd', required=True, type=str, help='Nombre de la base de datos hive (tabla de salida)')
parser.add_argument('--vfecha_fin', required=True, type=str,help='Parametro 1 de la query sql')
parser.add_argument('--vfecha_inicio', required=True, type=str,help='Parametro 2 de la query sql')
parser.add_argument('--vfecha_antes_ayer', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vultimo_dia_act_frmt', required=True, type=str,help='Parametro 3 de la query sql')

parametros = parser.parse_args()
vEntidad=parametros.ventidad
vBaseHive=parametros.vhivebd
vfecha_fin=parametros.vfecha_fin
vfecha_inicio=parametros.vfecha_inicio
vfecha_antes_ayer=parametros.vfecha_antes_ayer
vultimo_dia_act_frmt=parametros.vultimo_dia_act_frmt
vTablaFinal='db_cs_terminales.otc_t_ext_terminales_ajst'
## STEP 3: Inicio el SparkSession
spark = SparkSession \
    .builder \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.yarn.queue", "reportes") \
    .config("spark.rpc.askTimeout", "300s") \
    .appName(vEntidad) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId

##STEP 4:QUERYS
print(lne_dvs())
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
timestart_b = datetime.now()
try:
    vStp01="Paso 1"
    print(lne_dvs())
    print(etq_info("Paso [1]: Ejecucion de funcion [tmp_terminales_simcards_nc] - Tabla Temporal con terminales de notas de credito"))
    print(lne_dvs())
    df1=spark.sql(tmp_terminales_simcards_nc(vultimo_dia_act_frmt,vfecha_inicio,vfecha_fin)).cache()
    df1.printSchema()
    ts_step_tbl = datetime.now()
    df1.createOrReplaceTempView("tmp_terminales_simcards_nc")
    print(etq_info(msg_t_total_registros_obtenidos("df1",str(df1.count())))) 
    te_step_tbl = datetime.now()
    del df1
    print(etq_info(msg_d_duracion_hive("df1",vle_duracion(ts_step_tbl,te_step_tbl))))

    vStp01="Paso 2"
    print(lne_dvs())
    print(etq_info("Paso [2]: Ejecucion de funcion [tmp_terminales_simcards_factura] - Tabla Temporal de terminales con factura, es decir diferentes de notas de credito"))
    print(lne_dvs())
    df2=spark.sql(tmp_terminales_simcards_factura(vultimo_dia_act_frmt,vfecha_inicio,vfecha_fin)).cache()
    df2.printSchema()
    ts_step_tbl = datetime.now()
    df2.createOrReplaceTempView("tmp_terminales_simcards_factura")
    print(etq_info(msg_t_total_registros_obtenidos("df2",str(df2.count())))) 
    te_step_tbl = datetime.now()
    del df2
    print(etq_info(msg_d_duracion_hive("df2",vle_duracion(ts_step_tbl,te_step_tbl))))

    vStp01="Paso 3"
    print(lne_dvs())
    print(etq_info("Paso [3]: Ejecucion de funcion [tmp_terminales_simcards] - TABLA con la union de terminales NC y facturas"))
    print(lne_dvs())
    df3=spark.sql(tmp_terminales_simcards(vfecha_antes_ayer)).cache()
    df3.printSchema()
    ts_step_tbl = datetime.now()
    df3.createOrReplaceTempView("tmp_terminales_simcards")
    print(etq_info(msg_t_total_registros_obtenidos("df3",str(df3.count())))) 
    te_step_tbl = datetime.now()
    del df3
    print(etq_info(msg_d_duracion_hive("df3",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_terminales_simcards_nc")
    spark.catalog.dropTempView("tmp_terminales_simcards_factura")
    
    vStp01="Paso 4"
    print(lne_dvs())
    print(etq_info("Paso [4]: Ejecucion de funcion [otc_t_ext_terminales_ajst] - TABLA FINAL PARA REPORTE DE EXTRACTOR DE TERMINALES"))
    print(lne_dvs())
    df4=spark.sql(otc_t_ext_terminales_ajst()).cache()
    df4.printSchema()
    ts_step_tbl = datetime.now()
    df4.write.mode('overwrite').format('parquet').saveAsTable(vTablaFinal)
    print(etq_info(msg_t_total_registros_obtenidos("df4",str(df4.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df4",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_terminales_simcards")
    
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df4
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(msg_e_ejecucion(vStpFin,str(e)))

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())
