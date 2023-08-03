# -- coding: utf-8 --
import sys
reload(sys)
from query import *
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark_llap import HiveWarehouseSession
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
parser.add_argument('--vfecha_meses_atras2', required=True, type=str,help='Parametro 3 de la query sql')

parametros = parser.parse_args()
vEntidad=parametros.ventidad
vBaseHive=parametros.vhivebd
vfecha_fin=parametros.vfecha_fin
vfecha_meses_atras2=parametros.vfecha_meses_atras2
vTTempTermSCards='db_temporales.tmp_otc_t_terminales_simcards'

## STEP 3: Inicio el SparkSession
spark = SparkSession \
    .builder \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.rpc.askTimeout", "300s") \
    .appName(vEntidad) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
app_id = spark._sc.applicationId
hive_hwc = HiveWarehouseSession.session(spark).build()

##STEP 4:QUERYS
print(lne_dvs())
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
timestart_b = datetime.now()
try:
    print(lne_dvs())
    print(lne_dvs())
    vStp01="Paso 1"
    print(lne_dvs())
    print(etq_info("Paso [1]: Ejecucion de funcion [tmp_otc_t_terminales_simcards]- CREA TEMPORAL CON LA INFORMACION DE LA TABLA PRINCIPAL DE CS TERMINALES[TRANSACCIONAL]"))
    print(lne_dvs())
    df1=spark.sql(tmp_otc_t_terminales_simcards(vfecha_meses_atras2,vfecha_fin)).cache()
    df1.printSchema()
    ts_step_tbl = datetime.now()
    df1.write.mode('overwrite').format('parquet').saveAsTable(vTTempTermSCards)
    print(etq_info(msg_t_total_registros_obtenidos("df1",str(df1.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df1",vle_duracion(ts_step_tbl,te_step_tbl))))
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df1
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(msg_e_ejecucion(vStpFin,str(e)))

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())
