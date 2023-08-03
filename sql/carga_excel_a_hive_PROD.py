# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import pandas as pd
from pyspark.sql.types import *
import argparse

sys.path.append('/var/opt/tel_spark')

from messages import *
from functions import *
from create import *


def msg():
    return os.path.basename(__file__) + "[-f1] [-f2] [-tablaPoc] [-poc] [-tablaRutero]"



if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-base_datos", help="@Base_Datos", dest='base_datos', type=str)
    parser.add_argument("-ruta_archivo", help="@Ruta_Archivo", dest='ruta_archivo', type=str)
    parser.add_argument("-archivo_csv_1", help="@Archivo_Csv", dest='archivo_csv_1', type=str)
    parser.add_argument("-archivo_csv_2", help="@Archivo_Csv", dest='archivo_csv_2', type=str)
    parser.add_argument("-archivo_csv_3", help="@Archivo_Csv", dest='archivo_csv_3', type=str)
    parser.add_argument("-archivo_csv_4", help="@Archivo_Csv", dest='archivo_csv_4', type=str)
    parser.add_argument("-archivo_csv_5", help="@Archivo_Csv", dest='archivo_csv_5', type=str)
    parser.add_argument("-tabla_1", help="@Tabla", dest='tabla_1', type=str)
    parser.add_argument("-tabla_2", help="@Tabla", dest='tabla_2', type=str)
    parser.add_argument("-tabla_3", help="@Tabla", dest='tabla_3', type=str)
    parser.add_argument("-tabla_4", help="@Tabla", dest='tabla_4', type=str)
    parser.add_argument("-tabla_5", help="@Tabla", dest='tabla_5', type=str)
    
    
    args = parser.parse_args()
    p_ruta_archivo = args.ruta_archivo
    p_base_datos = args.base_datos
    p_archivo_csv_1 = args.archivo_csv_1
    p_archivo_csv_2 = args.archivo_csv_2
    p_archivo_csv_3 = args.archivo_csv_3
    p_archivo_csv_4 = args.archivo_csv_4
    p_archivo_csv_5 = args.archivo_csv_5
    p_tabla_1 = args.tabla_1
    p_tabla_2 = args.tabla_2
    p_tabla_3 = args.tabla_3
    p_tabla_4 = args.tabla_4
    p_tabla_5 = args.tabla_5
    
    val_cola_ejecucion = 'capa_semantica'
    configuracion = SparkConf().setAppName('PySpark_Terminales_Simcards'). \
    setAll([('spark.speculation', 'false'), ('spark.master', 'yarn'), ('hive.exec.dynamic.partition.mode', 'nonstrict'),('spark.yarn.queue',val_cola_ejecucion), ('hive.exec.dynamic.partition', 'true'), ('hive.enforce.bucketing', 'false'), ('hive.enforce.sorting', 'false')])

    sc = SparkContext(conf=configuracion)
    sc.getConf().getAll()
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)
    app_id = sqlContext._sc.applicationId
    
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
        print(etq_info(log_p_parametros("p_ruta_archivo",str(p_ruta_archivo))))
        print(etq_info(log_p_parametros("p_base_datos",str(p_base_datos))))
        print(etq_info(log_p_parametros("p_archivo_csv_1",str(p_archivo_csv_1))))
        print(etq_info(log_p_parametros("p_archivo_csv_2",str(p_archivo_csv_2))))
        print(etq_info(log_p_parametros("p_archivo_csv_3",str(p_archivo_csv_3))))
        print(etq_info(log_p_parametros("p_archivo_csv_4",str(p_archivo_csv_4))))
        print(etq_info(log_p_parametros("p_archivo_csv_5",str(p_archivo_csv_5))))
        print(etq_info(log_p_parametros("p_tabla_1",str(p_tabla_1))))  
        print(etq_info(log_p_parametros("p_tabla_2",str(p_tabla_2))))
        print(etq_info(log_p_parametros("p_tabla_3",str(p_tabla_3))))
        print(etq_info(log_p_parametros("p_tabla_4",str(p_tabla_4))))
        print(etq_info(log_p_parametros("p_tabla_5",str(p_tabla_5))))
        

        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(vStp00,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vStp00,str(e))))

    #---------------------------------------------------------vStp01------------------------------------------------------------------#
    #100 Nueva Categoria.xlsx otc_t_catalogo_terminales
    print(lne_dvs())
    vStp01='Paso [1]: 100 Nueva Categoria.xlsx otc_t_catalogo_terminales'
    try:
        ts_step = datetime.now()
        df_in_1 = pd.read_excel( (p_ruta_archivo + '/' + p_archivo_csv_1) )
        mySchema_1 = StructType([ StructField('modelo_scl', StringType(), True)\
                           ,StructField('tipos_de_servicio', StringType(), True)\
                           ,StructField('segmentacion_smarts', StringType(), True)\
                           ,StructField('fabricante', StringType(), True)\
                           ,StructField('modelo_guia_comercial', StringType(), True)\
                           ,StructField('gama_final', StringType(), True)\
                           ,StructField('descripcion_gama', StringType(), True)\
                           ,StructField('tipo_facturacion', StringType(), True)\
                           ,StructField('datos', StringType(), True)\
                           ,StructField('smart_o_no_smart', StringType(), True)\
                           ,StructField('2g_3g', StringType(), True)\
                           ,StructField('codigo_articulo', StringType(), True)\
                           ,StructField('nombre_business_intelligence', StringType(), True)\
                           ,StructField('tipo_de_correo', StringType(), True)])  

        sdf_in_1 = sqlContext.createDataFrame(df_in_1, schema = mySchema_1)
        sdf_in_1.printSchema()
        sdf_in_1.show(5)
        truncate_1="truncate table {}.{} ".format(p_base_datos,p_tabla_1)
        #sqlContext.sql("truncate table " + p_base_datos + '.' + p_tabla_1)
        sqlContext.sql(truncate_1)
        print(etq_info(truncate_1))
        
        sdf_in_1.write.mode("overwrite").saveAsTable( (p_base_datos + '.' + p_tabla_1), mode = 'overwrite')
        print(etq_info("Se inserto en la tabla: "+p_base_datos+'.'+p_tabla_1))
        conteo="select count(1) from {}.{}".format(p_base_datos,p_tabla_1)
        print(etq_info(conteo))
        df_conteo_01=sqlContext.sql(conteo)
        df_conteo = df_conteo_01.collect()[0][0]
     
        if df_conteo == 0:
            exit(etq_error(etq_nodata(msg_e_df_nodata(str('df_conteo')))))
        else:
            df_conteo_01.show()
            
        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

    #---------------------------------------------------------vStp02------------------------------------------------------------------#  
  
    #LISTADO_RUC_DAS_RETAIL.xlsx otc_t_catalogo_ruc_das_retail
    print(lne_dvs())
    vStp01='Paso [2]: LISTADO_RUC_DAS_RETAIL.xlsx otc_t_catalogo_ruc_das_retail'
    try:
        ts_step = datetime.now()    
        df_in_2 = pd.read_excel( (p_ruta_archivo + '/' + p_archivo_csv_2) )
        mySchema_2 = StructType([ StructField('ruc', StringType(), True)\
                           ,StructField('razon_social', StringType(), True)\
                           ,StructField('tipo_local', StringType(), True)\
                           ,StructField('zona', StringType(), True)\
                           ,StructField('codigo_da', StringType(), True)])  

        sdf_in_2 = sqlContext.createDataFrame(df_in_2, schema = mySchema_2)
        sdf_in_2.printSchema()
        sdf_in_2.show(5)
        truncate_2="truncate table {}.{} ".format(p_base_datos,p_tabla_2)
        #sqlContext.sql("truncate table " + p_base_datos + '.' + p_tabla_2)
        sqlContext.sql(truncate_2)
        print(etq_info(truncate_2))
        
        sdf_in_2.write.mode("overwrite").saveAsTable( (p_base_datos + '.' + p_tabla_2), mode = 'overwrite')
        print(etq_info("Se inserto en la tabla: "+p_base_datos+'.'+p_tabla_2))
        conteo="select count(1) from {}.{}".format(p_base_datos,p_tabla_2)
        print(etq_info(conteo))
        df_conteo_01=sqlContext.sql(conteo)
        df_conteo = df_conteo_01.collect()[0][0]
     
        if df_conteo == 0:
            exit(etq_error(etq_nodata(msg_e_df_nodata(str('df_conteo')))))
        else:
            df_conteo_01.show()
    
        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vStp01,str(e))))    
    #---------------------------------------------------------vStp03------------------------------------------------------------------# 
    #TIPO_CANAL.xlsx otc_t_catalogo_tipo_canal
    print(lne_dvs())
    vStp01='Paso [3]: TIPO_CANAL.xlsx otc_t_catalogo_tipo_canal'
    try:
        ts_step = datetime.now() 
        df_in_3 = pd.read_excel( (p_ruta_archivo + '/' + p_archivo_csv_3) )
        mySchema_3 = StructType([ StructField('canal', StringType(), True)\
                           ,StructField('tipo_canal', StringType(), True)])  

        sdf_in_3 = sqlContext.createDataFrame(df_in_3, schema = mySchema_3)
        sdf_in_3.printSchema()
        sdf_in_3.show(5)
        truncate_3="truncate table {}.{} ".format(p_base_datos,p_tabla_3)
        #sqlContext.sql("truncate table " + p_base_datos + '.' + p_tabla_3)
        sqlContext.sql(truncate_3)
        print(etq_info(truncate_3))
        
        sdf_in_3.write.mode("overwrite").saveAsTable( (p_base_datos + '.' + p_tabla_3), mode = 'overwrite')
        print(etq_info("Se inserto en la tabla: "+p_base_datos+'.'+p_tabla_3))
        conteo="select count(1) from {}.{}".format(p_base_datos,p_tabla_3)
        print(etq_info(conteo))
        df_conteo_01=sqlContext.sql(conteo)
        df_conteo = df_conteo_01.collect()[0][0]
     
        if df_conteo == 0:
            exit(etq_error(etq_nodata(msg_e_df_nodata(str('df_conteo')))))
        else:
            df_conteo_01.show()
        
        #sdf_in_3.write.mode("append").format("hive").saveAsTable( (p_base_datos + '.' + p_tabla_3), mode = 'append')

        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vStp01,str(e))))    

    #---------------------------------------------------------vStp04------------------------------------------------------------------# 
    #USUARIOS_CANAL_ONLINE.xlsx otc_t_catalogo_canal_online
    print(lne_dvs())
    vStp01='Paso [4]: USUARIOS_CANAL_ONLINE.xlsx otc_t_catalogo_canal_online'
    try:
        ts_step = datetime.now()
        df_in_4 = pd.read_excel( (p_ruta_archivo + '/' + p_archivo_csv_4) )
        mySchema_4 = StructType([ StructField('nae', StringType(), True)\
                           ,StructField('proveedor', StringType(), True)])  

        sdf_in_4 = sqlContext.createDataFrame(df_in_4, schema = mySchema_4)
        sdf_in_4.printSchema()
        sdf_in_4.show(5)
        truncate_4="truncate table {}.{} ".format(p_base_datos,p_tabla_4)
        #sqlContext.sql("truncate table " + p_base_datos + '.' + p_tabla_4)
        sqlContext.sql(truncate_4)
        print(etq_info(truncate_4))
        
        sdf_in_4.write.mode("overwrite").saveAsTable( (p_base_datos + '.' + p_tabla_4), mode = 'overwrite')
        print(etq_info("Se inserto en la tabla: "+p_base_datos+'.'+ p_tabla_4))
        conteo="select count(1) from {}.{}".format(p_base_datos,p_tabla_4)
        print(etq_info(conteo))
        df_conteo_01=sqlContext.sql(conteo)
        df_conteo = df_conteo_01.collect()[0][0]
     
        if df_conteo == 0:
            exit(etq_error(etq_nodata(msg_e_df_nodata(str('df_conteo')))))
        else:
            df_conteo_01.show()
            
        #sdf_in_4.write.mode("append").format("hive").saveAsTable( (p_base_datos + '.' + p_tabla_4), mode = 'append')
        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vStp01,str(e))))     
    
    
    #---------------------------------------------------------vStp05------------------------------------------------------------------# 
    #CONCEPTOS_FACT_ADICIONALES.xlsx otc_t_conceptos_fact_adicion
    print(lne_dvs())
    vStp01='Paso [4]: USUARIOS_CANAL_ONLINE.xlsx otc_t_catalogo_canal_online'
    try:
        ts_step = datetime.now()
        df_in_5 = pd.read_excel( (p_ruta_archivo + '/' + p_archivo_csv_5) )
        mySchema_5 = StructType([ StructField('revenue_code_id', StringType(), True)\
                           ,StructField('revenue_code_desc', StringType(), True)\
                           ,StructField('product_id', StringType(), True)\
                           ,StructField('product_desc', StringType(), True)\
                           ,StructField('clasificacion', StringType(), True)])

        sdf_in_5 = sqlContext.createDataFrame(df_in_5, schema = mySchema_5)
        sdf_in_5.printSchema()
        sdf_in_5.show(5)
        truncate_5="truncate table {}.{} ".format(p_base_datos,p_tabla_5)
        #sqlContext.sql("truncate table " + p_base_datos + '.' + p_tabla_5)
        sqlContext.sql(truncate_5)
        print(etq_info(truncate_5))
        
        sdf_in_5.write.mode("overwrite").saveAsTable( (p_base_datos + '.' + p_tabla_5), mode = 'overwrite')
        print(etq_info("Se inserto en la tabla: "+p_base_datos+'.'+ p_tabla_5))
        conteo="select count(1) from {}.{}".format(p_base_datos,p_tabla_5)
        print(etq_info(conteo))
        df_conteo_01=sqlContext.sql(conteo)
        df_conteo = df_conteo_01.collect()[0][0]
     
        if df_conteo == 0:
            exit(etq_error(etq_nodata(msg_e_df_nodata(str('df_conteo')))))
        else:
            df_conteo_01.show()
    
        #sdf_in_5.write.mode("append").format("hive").saveAsTable( (p_base_datos + '.' + p_tabla_5), mode = 'append') 
        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vStp01,str(e))))     
        
    print(lne_dvs())
    vStpFin='Paso [Final]: Eliminando dataframes ..'
    print(lne_dvs())

    try:
        ts_step = datetime.now()    
        del df_conteo_01
        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))
        
    finally:
        sqlContext.clearCache()
        sc.stop()

        print(lne_dvs())        
