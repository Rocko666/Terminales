# -*- coding: latin1 -*-
import sys
reload(sys)
sys.setdefaultencoding('latin1')
#sys.setdefaultencoding('windows-1252')
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F, Window
import re
from pyspark.sql.functions import *
import argparse
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *

parser = argparse.ArgumentParser()
parser.add_argument('--rutain', required=True, type=str)
parser.add_argument('--tablaout', required=True, type=str)
parser.add_argument('--tipo', required=True, type=str)
parametros = parser.parse_args()
vPathExcel=parametros.rutain
vTablaOut=parametros.tablaout
vTipo=parametros.tipo

timestart = datetime.now()
vRegExpUnnamed=r"unnamed*"
vApp="ReadExcel"

def getColumnName(vColumn=str):
    a=vColumn.lower()
    x=a.replace(' ','_')
    y=x.replace(':','')
    return y

spark = SparkSession\
    .builder\
    .appName(vApp)\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId

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
    print(etq_info(log_p_parametros("vApp",str(vApp))))
    print(etq_info(log_p_parametros("vRegExpUnnamed",str(vRegExpUnnamed))))
    print(etq_info(log_p_parametros("vPathExcel",str(vPathExcel))))
    print(etq_info(log_p_parametros("vTablaOut",str(vTablaOut))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp00,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp00,str(e))))

print(lne_dvs())
vStp01='Paso [1]: Se extrae la extension del archivo..'
try:
    ts_step = datetime.now()
    dfExcel = pd.read_excel(vPathExcel, error_bad_lines=False, dtype=str,encoding='latin1')
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStp02='Paso [2]: Guardar el contenido del file en una tabla en Hive'
try:
    df0 = spark.createDataFrame(dfExcel.astype(str))
    df1 = df0.drop('TICKET','USUARIO_FINAL.1')
    df2 = df1.drop('TICKET ')
    df2 = df2.withColumn('FECHA_PROCESO',date_format(df2.FECHA_PROCESO,'dd/MM/yyyy').alias('dd/MM/yyyy') )
    df2 = df2.withColumn('FECHA_FACTURA',date_format(df2.FECHA_FACTURA,'dd/MM/yyyy').alias('dd/MM/yyyy') )
    if df2.rdd.isEmpty():
        raise Exception("No hay datos a cargar")
    else:
        df3 = df2.withColumn("fechacarga",F.lit(datetime.now()))
        print(etq_info("Las columnas encontradas son:"))
        print(df3.printSchema())
        df3.write.mode(vTipo).format('parquet').saveAsTable(vTablaOut)
        print(etq_info(msg_t_total_registros_hive(vTablaOut,str(df3.count())))) #BORRAR
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp02,str(e))))
    
print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion('otc_t_ajustes_terminales.py',vle_duracion(timestart,timeend))))
print(lne_dvs())

