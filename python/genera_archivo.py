# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from query import *
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *

## STEP 1: Definir variables o constantes
vLogInfo='INFO:'
vLogError='ERROR:'

timestart = datetime.now()
## STEP 2: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=False, type=str,help='Parametro de la entidad')
parser.add_argument('--vval_fecha_formato', required=True, type=str,help='Parametro 1 de la query sql')
parser.add_argument('--vval_dia_uno', required=True, type=str,help='Parametro 2 de la query sql')
parser.add_argument('--vfecha_inicio', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vArchivo', required=True, type=str, help='Nombre del archivo final')

parametros = parser.parse_args()
vEntidad=parametros.ventidad
vArchivo=parametros.vArchivo
vval_fecha_formato=parametros.vval_fecha_formato
vval_dia_uno=parametros.vval_dia_uno
vfecha_inicio=parametros.vfecha_inicio

## STEP 3: Inicio el SparkSession
spark = SparkSession \
    .builder \
    .config("spark.driver.maxResultSize", "4g") \
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
    vStp01="GENERACION DE ARCHIVO"
    print(lne_dvs())
    print(lne_dvs())
    print(etq_info(vStp01))
    print(lne_dvs())
    print(lne_dvs())
       
    df_arch1 = spark.sql(sql_file(vval_fecha_formato,vval_dia_uno,vfecha_inicio))
    pandas_df1 = df_arch1.toPandas()
    pandas_df1.to_csv(vArchivo, sep='|',index=False)   
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()    
    del df_arch1, pandas_df1
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(msg_e_ejecucion(vStpFin,str(e)))

spark.stop()
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())

