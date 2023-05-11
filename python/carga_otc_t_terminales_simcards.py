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
import subprocess
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

## STEP 1: Definir variables o constantes
vLogInfo='INFO:'
vLogError='ERROR:'
vTablaCostFinV2="db_desarrollo2021.tmp_costo_fact_final_v2_1_csts"
VtablaTmpCostFactFin="db_desarrollo2021.tmp_costo_fac_final_csts"
VtablaTmpFactMov="db_desarrollo2021.tmp_fact_mov_final_csts"

timestart = datetime.now()
## STEP 2: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=False, type=str,help='Parametro de la entidad')
parser.add_argument('--vhivebd', required=True, type=str, help='Nombre de la base de datos hive (tabla de salida)')
parser.add_argument('--vfecha_fin', required=True, type=str,help='Parametro 1 de la query sql')
parser.add_argument('--vfecha_inicio', required=True, type=str,help='Parametro 2 de la query sql')
parser.add_argument('--vfecha_antes_ayer', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vdia_uno_mes_sig_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vultimo_dia_act_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vanio_mes', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vsolo_anio', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vsolo_mes', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vfecha_meses_atras', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vfecha_meses_atras1', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vfecha_meses_atras2', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vdia_uno_mes_act_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vdia_uno_mes_ant_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vval_usuario4', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vval_usuario_final', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vTablaDestino', required=True, type=str,help='Parametro 3 de la query sql')

parametros = parser.parse_args()
vEntidad=parametros.ventidad
vBaseHive=parametros.vhivebd
vfecha_fin=parametros.vfecha_fin
vfecha_inicio=parametros.vfecha_inicio
vfecha_antes_ayer=parametros.vfecha_antes_ayer
vdia_uno_mes_sig_frmt=parametros.vdia_uno_mes_sig_frmt
vultimo_dia_act_frmt=parametros.vultimo_dia_act_frmt
vanio_mes=parametros.vanio_mes
vsolo_anio=parametros.vsolo_anio
vsolo_mes=parametros.vsolo_mes
vfecha_meses_atras=parametros.vfecha_meses_atras
vfecha_meses_atras1=parametros.vfecha_meses_atras1
vfecha_meses_atras2=parametros.vfecha_meses_atras2
vdia_uno_mes_act_frmt=parametros.vdia_uno_mes_act_frmt
vdia_uno_mes_ant_frmt=parametros.vdia_uno_mes_ant_frmt
vval_usuario4=parametros.vval_usuario4
vval_usuario_final=parametros.vval_usuario_final
vTablaDestino=parametros.vTablaDestino

## STEP 3: Inicio el SparkSession
spark = SparkSession \
    .builder \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.rpc.askTimeout", "300s") \
    .appName(vEntidad) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
sc.setLogLevel("ERROR")
hive_hwc = HiveWarehouseSession.session(spark).build()
app_id = spark._sc.applicationId

##STEP 4:QUERYS
print(lne_dvs())
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
timestart_b = datetime.now()
try:
    print(lne_dvs())
    print(lne_dvs())
    vStp01="Paso 1"
    print(lne_dvs())
    print(etq_info("Paso [1]: Ejecucion de funcion [tmp_catalogo_terminales_csts]- CREA TEMPORAL CON ALGUNAS TRANSFORMACIONES A CAMPOS DEL CATALOGO DE TERMINALES"))
    print(lne_dvs())
    df_tmp_catalogo_terminales_csts=spark.sql(tmp_catalogo_terminales_csts()).cache()
    df_tmp_catalogo_terminales_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_catalogo_terminales_csts.createOrReplaceTempView("tmp_catalogo_terminales_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_catalogo_terminales_csts",str(df_tmp_catalogo_terminales_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_catalogo_terminales_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 2"
    print(lne_dvs())
    print(etq_info("Paso [2]: Ejecucion de funcion [tmp_facturas_csts]- CREA TEMPORAL CON EL UNIVERSO PRINCIPAL DE LA FACTURACION"))
    print(lne_dvs())
    df_tmp_facturas_csts=spark.sql(tmp_facturas_csts(vfecha_inicio,vfecha_fin)).cache()
    df_tmp_facturas_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_facturas_csts.createOrReplaceTempView("tmp_facturas_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_facturas_csts",str(df_tmp_facturas_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_facturas_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 3"
    print(lne_dvs())
    print(etq_info("Paso [3]: Ejecucion de funcion [tmp_notas_credito_csts]- UNIVERSO PRINCIPAL DE NOTAS CREDITO"))
    print(lne_dvs())
    df_tmp_notas_credito_csts=spark.sql(tmp_notas_credito_csts(vfecha_inicio,vfecha_fin)).cache()
    df_tmp_notas_credito_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_notas_credito_csts.createOrReplaceTempView("tmp_notas_credito_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_notas_credito_csts",str(df_tmp_notas_credito_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_notas_credito_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 4"
    print(lne_dvs())
    print(etq_info("Paso [4]: Ejecucion de funcion [tmp_orden_venta_csts]-INFORMACION DE ORDEN DE VENTA PARA LOS type_id = 9062352550013045460"))
    print(lne_dvs())
    df_tmp_orden_venta_csts=spark.sql(tmp_orden_venta_csts(vfecha_meses_atras,vfecha_fin)).cache()
    df_tmp_orden_venta_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_orden_venta_csts.createOrReplaceTempView("tmp_orden_venta_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_orden_venta_csts",str(df_tmp_orden_venta_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_orden_venta_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 5"
    print(lne_dvs())
    print(etq_info("Paso [5]: Ejecucion de funcion [tmp_imei_articulo_csts]- INFORMACION DEL ARTICULO PARA CADA IMEI"))
    print(lne_dvs())
    df_tmp_imei_articulo_csts=spark.sql(tmp_imei_articulo_csts()).cache()
    df_tmp_imei_articulo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_imei_articulo_csts.createOrReplaceTempView("tmp_imei_articulo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_imei_articulo_csts",str(df_tmp_imei_articulo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_imei_articulo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 6"
    print(lne_dvs())
    print(etq_info("Paso [6]: Ejecucion de funcion [tmp_facturacion_usuario_csts]- INFORMACION DE LA FACTURACION CRUZADA CON USUARIO"))
    print(lne_dvs())
    df_tmp_facturacion_usuario_csts=spark.sql(tmp_facturacion_usuario_csts()).cache()
    df_tmp_facturacion_usuario_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_facturacion_usuario_csts.createOrReplaceTempView("tmp_facturacion_usuario_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_facturacion_usuario_csts",str(df_tmp_facturacion_usuario_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_facturacion_usuario_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_facturas_csts") 
    
    vStp01="Paso 7"
    print(lne_dvs())
    print(etq_info("Paso [7]: Ejecucion de funcion [tmp_nota_credito_usuario_csts] -INFORMACION DE NOTA CREDITO CRUZADA CON USUARIO"))
    print(lne_dvs())
    df_tmp_nota_credito_usuario_csts=spark.sql(tmp_nota_credito_usuario_csts()).cache()
    df_tmp_nota_credito_usuario_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_nota_credito_usuario_csts.createOrReplaceTempView("tmp_nota_credito_usuario_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_nota_credito_usuario_csts",str(df_tmp_nota_credito_usuario_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_nota_credito_usuario_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_notas_credito_csts")
    
    vStp01="Paso 8"
    print(lne_dvs())
    print(etq_info("Paso [8]: Ejecucion de funcion [tmp_une_fact_notascred_csts]- UNION DE FACTURAS Y NOTAS CREDITO"))
    print(lne_dvs())
    df_tmp_une_fact_notascred_csts=spark.sql(tmp_une_fact_notascred_csts()).cache()
    df_tmp_une_fact_notascred_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_une_fact_notascred_csts.createOrReplaceTempView("tmp_une_fact_notascred_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_une_fact_notascred_csts",str(df_tmp_une_fact_notascred_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_une_fact_notascred_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_facturacion_usuario_csts")
    spark.catalog.dropTempView("tmp_nota_credito_usuario_csts")
    
    vStp01="Paso 9"
    print(lne_dvs())
    print(etq_info("Paso [9]: Ejecucion de funcion [tmp_clientes_csts] - INFORMACION DE CLIENTES"))
    print(lne_dvs())
    df_tmp_clientes_csts=spark.sql(tmp_clientes_csts(vfecha_antes_ayer)).cache()
    df_tmp_clientes_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_clientes_csts.createOrReplaceTempView("tmp_clientes_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_clientes_csts",str(df_tmp_clientes_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_clientes_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 10"
    print(lne_dvs())
    print(etq_info("Paso [10]: Ejecucion de funcion [tmp_clientes_categoria_csts] - INFORMACION DE CLIENTES POR CATEGORIA"))
    print(lne_dvs())
    df_tmp_clientes_categoria_csts=spark.sql(tmp_clientes_categoria_csts()).cache()
    df_tmp_clientes_categoria_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_clientes_categoria_csts.createOrReplaceTempView("tmp_clientes_categoria_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_clientes_categoria_csts",str(df_tmp_clientes_categoria_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_clientes_categoria_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_une_fact_notascred_csts")
    spark.catalog.dropTempView("tmp_clientes_csts")
    
    vStp01="Paso 11"
    print(lne_dvs())
    print(etq_info("Paso [11]: Ejecucion de funcion [tmp_imei_cod_articulo_csts] - INFORMACION DE IMEI CON CODIGO ARTICULO"))
    print(lne_dvs())
    df_tmp_imei_cod_articulo_csts=spark.sql(tmp_imei_cod_articulo_csts()).cache()
    df_tmp_imei_cod_articulo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_imei_cod_articulo_csts.createOrReplaceTempView("tmp_imei_cod_articulo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_imei_cod_articulo_csts",str(df_tmp_imei_cod_articulo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_imei_cod_articulo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 12"
    print(lne_dvs())
    print(etq_info("Paso [12]: Ejecucion de funcion [tmp_agrupa_imei_codart_csts]- INFORMACION AGRUPADA DE IMEI CON CODIGO ARTICULO"))
    print(lne_dvs())
    df_tmp_agrupa_imei_codart_csts=spark.sql(tmp_agrupa_imei_codart_csts()).cache()
    df_tmp_agrupa_imei_codart_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_agrupa_imei_codart_csts.createOrReplaceTempView("tmp_agrupa_imei_codart_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_agrupa_imei_codart_csts",str(df_tmp_agrupa_imei_codart_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_agrupa_imei_codart_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_imei_cod_articulo_csts")
    
    vStp01="Paso 13"
    print(lne_dvs())
    print(etq_info("Paso [13]: Ejecucion de funcion [tmp_resto_segmento_csts] - INFORMACION PARA EL RESTO DE SEGMENTOS"))
    print(lne_dvs())
    df_tmp_resto_segmento_csts=spark.sql(tmp_resto_segmento_csts()).cache()
    df_tmp_resto_segmento_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_resto_segmento_csts.createOrReplaceTempView("tmp_resto_segmento_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_resto_segmento_csts",str(df_tmp_resto_segmento_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_resto_segmento_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_clientes_categoria_csts")
    spark.catalog.dropTempView("tmp_agrupa_imei_codart_csts")
    
    vStp01="Paso 14"
    print(lne_dvs())
    print(etq_info("Paso [14]: Ejecucion de funcion [tmp_terminal_equipo_csts] -  INFORMACION DEL UNIVERSO PRINCIPAL CON EL CATALOGO DE TERMINALES"))
    print(lne_dvs())
    df_tmp_terminal_equipo_csts=spark.sql(tmp_terminal_equipo_csts()).cache()
    df_tmp_terminal_equipo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_terminal_equipo_csts.createOrReplaceTempView("tmp_terminal_equipo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_terminal_equipo_csts",str(df_tmp_terminal_equipo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_terminal_equipo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_catalogo_terminales_csts")
    spark.catalog.dropTempView("tmp_resto_segmento_csts")
    
    
    vStp01="Paso 15"
    print(lne_dvs())
    print(etq_info("Paso [15]: Ejecucion de funcion [tmp_usuario_cm_csts] - INFORMACION DE USUARIOS SIN DUPLICADOS ANTES DEL PRIMER DIA DEL SIGUIENTE MES"))
    print(lne_dvs())
    df_tmp_usuario_cm_csts=spark.sql(tmp_usuario_cm_csts(vdia_uno_mes_sig_frmt)).cache()
    df_tmp_usuario_cm_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_usuario_cm_csts.createOrReplaceTempView("tmp_usuario_cm_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_usuario_cm_csts",str(df_tmp_usuario_cm_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_usuario_cm_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
       
    
    vStp01="Paso 16"
    print(lne_dvs())
    print(etq_info("Paso [16]: Ejecucion de funcion [tmp_trmnl_eqp_canal_usu_csts] - INFORMACION DE ORDEN DE VENTA CON NUEVO CAMPO DE CRUCE"))
    print(lne_dvs())
    df_tmp_trmnl_eqp_canal_usu_csts=spark.sql(tmp_trmnl_eqp_canal_usu_csts()).cache()
    df_tmp_trmnl_eqp_canal_usu_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_trmnl_eqp_canal_usu_csts.createOrReplaceTempView("tmp_trmnl_eqp_canal_usu_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_trmnl_eqp_canal_usu_csts",str(df_tmp_trmnl_eqp_canal_usu_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_trmnl_eqp_canal_usu_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_orden_venta_csts")
    spark.catalog.dropTempView("tmp_terminal_equipo_csts")
    
    vStp01="Paso 17"
    print(lne_dvs())
    print(etq_info("Paso [17]: Ejecucion de funcion [tmp_trmneqp_fuente_canal_csts] - INFORMACION DEL UNIVERSO PRINCIPAL ANTERIOR CON LA FUENTE DEL CANAL, BRANCH Y TIENDA"))
    print(lne_dvs())
    df_tmp_trmneqp_fuente_canal_csts=spark.sql(tmp_trmneqp_fuente_canal_csts()).cache()
    df_tmp_trmneqp_fuente_canal_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_trmneqp_fuente_canal_csts.createOrReplaceTempView("tmp_trmneqp_fuente_canal_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_trmneqp_fuente_canal_csts",str(df_tmp_trmneqp_fuente_canal_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_trmneqp_fuente_canal_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_trmnl_eqp_canal_usu_csts")
    
    vStp01="Paso 18"
    print(lne_dvs())
    print(etq_info("Paso [18]: Ejecucion de funcion [tmp_trmneqp_cruza_ruc_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON EL RUC"))
    print(lne_dvs())
    df_tmp_trmneqp_cruza_ruc_csts=spark.sql(tmp_trmneqp_cruza_ruc_csts()).cache()
    df_tmp_trmneqp_cruza_ruc_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_trmneqp_cruza_ruc_csts.createOrReplaceTempView("tmp_trmneqp_cruza_ruc_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_trmneqp_cruza_ruc_csts",str(df_tmp_trmneqp_cruza_ruc_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_trmneqp_cruza_ruc_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_trmneqp_fuente_canal_csts")
    
    vStp01="Paso 19"
    print(lne_dvs())
    print(etq_info("Paso [19]: Ejecucion de funcion [tmp_trmneqp_update_canal_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON EL RUC"))
    print(lne_dvs())
    df_tmp_trmneqp_update_canal_csts=spark.sql(tmp_trmneqp_update_canal_csts()).cache()
    df_tmp_trmneqp_update_canal_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_trmneqp_update_canal_csts.createOrReplaceTempView("tmp_trmneqp_update_canal_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_trmneqp_update_canal_csts",str(df_tmp_trmneqp_update_canal_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_trmneqp_update_canal_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_trmneqp_cruza_ruc_csts")
    
    vStp01="Paso 20"
    print(lne_dvs())
    print(etq_info("Paso [20]: Ejecucion de funcion [tmp_trmneqp_full_name_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON EL RUC"))
    print(lne_dvs())
    df_tmp_trmneqp_full_name_csts=spark.sql(tmp_trmneqp_full_name_csts()).cache()
    df_tmp_trmneqp_full_name_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_trmneqp_full_name_csts.createOrReplaceTempView("tmp_trmneqp_full_name_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_trmneqp_full_name_csts",str(df_tmp_trmneqp_full_name_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_trmneqp_full_name_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_trmneqp_update_canal_csts")
    
    vStp01="Paso 21"
    print(lne_dvs())
    print(etq_info("Paso [21]: Ejecucion de funcion [tmp_mov_parque_completa_csts] - INFORMACION DE LA MOVIPARQUE COMPLETA PARQUE ACTUAL"))
    print(lne_dvs())
    df_tmp_mov_parque_completa_csts=spark.sql(tmp_mov_parque_completa_csts(vfecha_fin)).cache()
    df_tmp_mov_parque_completa_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mov_parque_completa_csts.createOrReplaceTempView("tmp_mov_parque_completa_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mov_parque_completa_csts",str(df_tmp_mov_parque_completa_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mov_parque_completa_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 22"
    print(lne_dvs())
    print(etq_info("Paso [22]: Ejecucion de funcion [tmp_mov_parque_completa_ant_csts] - INFORMACION DE LA MOVIPARQUE COMPLETA PARQUE ANTIGUO"))
    print(lne_dvs())
    df_tmp_mov_parque_completa_ant_csts=spark.sql(tmp_mov_parque_completa_ant_csts(vfecha_inicio)).cache()
    df_tmp_mov_parque_completa_ant_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mov_parque_completa_ant_csts.createOrReplaceTempView("tmp_mov_parque_completa_ant_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mov_parque_completa_ant_csts",str(df_tmp_mov_parque_completa_ant_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mov_parque_completa_ant_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 23"
    print(lne_dvs())
    print(etq_info("Paso [23]: Ejecucion de funcion [tmp_mov_parque_sin_dup_csts] - INFORMACION DE LA MOVIPARQUE SIN DUPLICADOS"))
    print(lne_dvs())
    df_tmp_mov_parque_sin_dup_csts=spark.sql(tmp_mov_parque_sin_dup_csts()).cache()
    df_tmp_mov_parque_sin_dup_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mov_parque_sin_dup_csts.createOrReplaceTempView("tmp_mov_parque_sin_dup_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mov_parque_sin_dup_csts",str(df_tmp_mov_parque_sin_dup_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mov_parque_sin_dup_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 24"
    print(lne_dvs())
    print(etq_info("Paso [24]: Ejecucion de funcion [tmp_mov_parque_ant_sin_dup_csts] -  INFORMACION DE LA MOVIPARQUE SIN DUPLICADOS PARQUE ANTIGUO"))
    print(lne_dvs())
    df_tmp_mov_parque_ant_sin_dup_csts=spark.sql(tmp_mov_parque_ant_sin_dup_csts()).cache()
    df_tmp_mov_parque_ant_sin_dup_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mov_parque_ant_sin_dup_csts.createOrReplaceTempView("tmp_mov_parque_ant_sin_dup_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mov_parque_ant_sin_dup_csts",str(df_tmp_mov_parque_ant_sin_dup_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mov_parque_ant_sin_dup_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_mov_parque_completa_ant_csts")
    
    vStp01="Paso 25"
    print(lne_dvs())
    print(etq_info("Paso [25]: Ejecucion de funcion [tmp_movparque_sin_dup_csts] - INFORMACION DE LA MOVIPARQUE SIN DUPLICADOS"))
    print(lne_dvs())
    df_tmp_movparque_sin_dup_csts=spark.sql(tmp_movparque_sin_dup_csts()).cache()
    df_tmp_movparque_sin_dup_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_movparque_sin_dup_csts.createOrReplaceTempView("tmp_movparque_sin_dup_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_movparque_sin_dup_csts",str(df_tmp_movparque_sin_dup_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_movparque_sin_dup_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_mov_parque_completa_csts")
    
    #aqui lo crea pero no lo usa
    vStp01="Paso 26"
    print(lne_dvs())
    print(etq_info("Paso [26]: Ejecucion de funcion [tmp_movparque_seg_csts] - INFORMACION DE LA MOVIPARQUE CRUZANDO CON EL CATALOGO PARA TRAER EL CAMPO SEGMENTO"))
    print(lne_dvs())
    df_tmp_movparque_seg_csts=spark.sql(tmp_movparque_seg_csts()).cache()
    df_tmp_movparque_seg_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_movparque_seg_csts.createOrReplaceTempView("tmp_movparque_seg_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_movparque_seg_csts",str(df_tmp_movparque_seg_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_movparque_seg_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 27"
    print(lne_dvs())
    print(etq_info("Paso [27]: Ejecucion de funcion [tmp_universo_ppal_mov_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CRUZADO CON LA MOVIPARQUE PARA OBTENER LINEA NEGOCIO, SEGMENTO Y SUBSEGMENTO"))
    print(lne_dvs())
    df_tmp_universo_ppal_mov_csts=spark.sql(tmp_universo_ppal_mov_csts()).cache()
    df_tmp_universo_ppal_mov_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_universo_ppal_mov_csts.createOrReplaceTempView("tmp_universo_ppal_mov_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_universo_ppal_mov_csts",str(df_tmp_universo_ppal_mov_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_universo_ppal_mov_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_trmneqp_full_name_csts")
    
    vStp01="Paso 28"
    print(lne_dvs())
    print(etq_info("Paso [28]: Ejecucion de funcion [tmp_mov_parque_antiguo_csts] - INFORMACION DEL PARQUE ANTIGUO"))
    print(lne_dvs())
    df_tmp_mov_parque_antiguo_csts=spark.sql(tmp_mov_parque_antiguo_csts(vdia_uno_mes_act_frmt,vdia_uno_mes_ant_frmt)).cache()
    df_tmp_mov_parque_antiguo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mov_parque_antiguo_csts.createOrReplaceTempView("tmp_mov_parque_antiguo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mov_parque_antiguo_csts",str(df_tmp_mov_parque_antiguo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mov_parque_antiguo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_mov_parque_ant_sin_dup_csts")
    
    vStp01="Paso 29"
    print(lne_dvs())
    print(etq_info("Paso [29]: Ejecucion de funcion [tmp_mov_parque_nuevo_csts] - INFORMACION DEL PARQUE NUEVO"))
    print(lne_dvs())
    df_tmp_mov_parque_nuevo_csts=spark.sql(tmp_mov_parque_nuevo_csts(vdia_uno_mes_act_frmt)).cache()
    df_tmp_mov_parque_nuevo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mov_parque_nuevo_csts.createOrReplaceTempView("tmp_mov_parque_nuevo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mov_parque_nuevo_csts",str(df_tmp_mov_parque_nuevo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mov_parque_nuevo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 30"
    print(lne_dvs())
    print(etq_info("Paso [30]: Ejecucion de funcion [tmp_movimientos_bi_csts]- INFORMACION DEL CAMPO FUENTE MOVIMIENTO DE ALTAS"))
    print(lne_dvs())
    df_tmp_movimientos_bi_csts=spark.sql(tmp_movimientos_bi_csts(vfecha_fin)).cache()
    df_tmp_movimientos_bi_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_movimientos_bi_csts.createOrReplaceTempView("tmp_movimientos_bi_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_movimientos_bi_csts",str(df_tmp_movimientos_bi_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_movimientos_bi_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 31"
    print(lne_dvs())
    print(etq_info("Paso [31]: Ejecucion de funcion [tmp_trmneqp_movimiento_csts] -  UNIVERSO PRINCIPAL CON INFORMACION CON EL MOVIMIENTO"))
    print(lne_dvs())
    df_tmp_trmneqp_movimiento_csts=spark.sql(tmp_trmneqp_movimiento_csts()).cache()
    df_tmp_trmneqp_movimiento_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_trmneqp_movimiento_csts.createOrReplaceTempView("tmp_trmneqp_movimiento_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_trmneqp_movimiento_csts",str(df_tmp_trmneqp_movimiento_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_trmneqp_movimiento_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_universo_ppal_mov_csts")
    
    
    vStp01="Paso 32"
    print(lne_dvs())
    print(etq_info("Paso [32]: Ejecucion de funcion [tmp_cuentas_un_min_csts] - INFORMACION DE CUENTAS CON UN SOLO NUMERO TELEFONICO"))
    print(lne_dvs())
    df_tmp_cuentas_un_min_csts=spark.sql(tmp_cuentas_un_min_csts()).cache()
    df_tmp_cuentas_un_min_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_cuentas_un_min_csts.createOrReplaceTempView("tmp_cuentas_un_min_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_cuentas_un_min_csts",str(df_tmp_cuentas_un_min_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_cuentas_un_min_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 33"
    print(lne_dvs())
    print(etq_info("Paso [33]: Ejecucion de funcion [tmp_mines1_cuenta_un_min_csts] - INFORMACION DE MOVIPARQUE SIN MIN"))
    print(lne_dvs())
    df_tmp_mines1_cuenta_un_min_csts=spark.sql(tmp_mines1_cuenta_un_min_csts()).cache()
    df_tmp_mines1_cuenta_un_min_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mines1_cuenta_un_min_csts.createOrReplaceTempView("tmp_mines1_cuenta_un_min_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mines1_cuenta_un_min_csts",str(df_tmp_mines1_cuenta_un_min_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mines1_cuenta_un_min_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_cuentas_un_min_csts")
    
    vStp01="Paso 34"
    print(lne_dvs())
    print(etq_info("Paso [34]: Ejecucion de funcion [tmp_mov_mines_bi_csts] - UNIVERSO PRINCIPAL CON INFORMACION DE MOVIMIENTOS CON MINES Y MOV BI"))
    print(lne_dvs())
    df_tmp_mov_mines_bi_csts=spark.sql(tmp_mov_mines_bi_csts()).cache()
    df_tmp_mov_mines_bi_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mov_mines_bi_csts.createOrReplaceTempView("tmp_mov_mines_bi_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mov_mines_bi_csts",str(df_tmp_mov_mines_bi_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mov_mines_bi_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_trmneqp_movimiento_csts")
    spark.catalog.dropTempView("tmp_mines1_cuenta_un_min_csts")
    spark.catalog.dropTempView("tmp_movimientos_bi_csts")
    
    vStp01="Paso 35"
    print(lne_dvs())
    print(etq_info("Paso [35]: Ejecucion de funcion [tmp_fact_mov_pre_csts] - INFORMACION DEL PARQUE NUEVO"))
    print(lne_dvs())
    df_tmp_fact_mov_pre_csts=spark.sql(tmp_fact_mov_pre_csts()).cache()
    df_tmp_fact_mov_pre_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_mov_pre_csts.createOrReplaceTempView("tmp_fact_mov_pre_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_mov_pre_csts",str(df_tmp_fact_mov_pre_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_mov_pre_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_mov_mines_bi_csts")
    spark.catalog.dropTempView("tmp_mov_parque_nuevo_csts")
    
    vStp01="Paso 36"
    print(lne_dvs())
    print(etq_info("Paso [36]: Ejecucion de funcion [tmp_cuentas_sin_min_csts] - INFORMACION DE CUENTAS SIN NUMERO TELEFONICO"))
    print(lne_dvs())
    df_tmp_cuentas_sin_min_csts=spark.sql(tmp_cuentas_sin_min_csts()).cache()
    df_tmp_cuentas_sin_min_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_cuentas_sin_min_csts.createOrReplaceTempView("tmp_cuentas_sin_min_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_cuentas_sin_min_csts",str(df_tmp_cuentas_sin_min_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_cuentas_sin_min_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 37"
    print(lne_dvs())
    print(etq_info("Paso [37]: Ejecucion de funcion [tmp_cuentas_completa_csts] - INFORMACION DE CUENTAS COMPLETA"))
    print(lne_dvs())
    df_tmp_cuentas_completa_csts=spark.sql(tmp_cuentas_completa_csts()).cache()
    df_tmp_cuentas_completa_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_cuentas_completa_csts.createOrReplaceTempView("tmp_cuentas_completa_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_cuentas_completa_csts",str(df_tmp_cuentas_completa_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_cuentas_completa_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_cuentas_sin_min_csts")
    spark.catalog.dropTempView("tmp_movparque_sin_dup_csts")
    
    vStp01="Paso 38"
    print(lne_dvs())
    print(etq_info("Paso [38]: Ejecucion de funcion [tmp_fact_ppal_completa_csts] - ACTUALIZACION DE LOS CAMPOS SEGMENTO, SUBSEGMENTO, LINEA_NEGOCIO, TELEFONO, ABONADO Y bnd_ln"))
    print(lne_dvs())
    df_tmp_fact_ppal_completa_csts=spark.sql(tmp_fact_ppal_completa_csts()).cache()
    df_tmp_fact_ppal_completa_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_ppal_completa_csts.createOrReplaceTempView("tmp_fact_ppal_completa_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_ppal_completa_csts",str(df_tmp_fact_ppal_completa_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_ppal_completa_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_mov_pre_csts")
    spark.catalog.dropTempView("tmp_cuentas_completa_csts")
    
    vStp01="Paso 39"
    print(lne_dvs())
    print(etq_info("Paso [39]: Ejecucion de funcion [tmp_cuenta_segmento_csts] - INFORMACION DE TODAS LAS CUENTAS CON 1 O VARIOS NUMEROS DE TELEFONO"))
    print(lne_dvs())
    df_tmp_cuenta_segmento_csts=spark.sql(tmp_cuenta_segmento_csts()).cache()
    df_tmp_cuenta_segmento_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_cuenta_segmento_csts.createOrReplaceTempView("tmp_cuenta_segmento_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_cuenta_segmento_csts",str(df_tmp_cuenta_segmento_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_cuenta_segmento_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 40"
    print(lne_dvs())
    print(etq_info("Paso [40]: Ejecucion de funcion [tmp_cuenta_seg_masivo_csts] - INFORMACION DE TODAS LAS CUENTAS MASIVO"))
    print(lne_dvs())
    df_tmp_cuenta_seg_masivo_csts=spark.sql(tmp_cuenta_seg_masivo_csts()).cache()
    df_tmp_cuenta_seg_masivo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_cuenta_seg_masivo_csts.createOrReplaceTempView("tmp_cuenta_seg_masivo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_cuenta_seg_masivo_csts",str(df_tmp_cuenta_seg_masivo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_cuenta_seg_masivo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 41"
    print(lne_dvs())
    print(etq_info("Paso [41]: Ejecucion de funcion [tmp_universo_fact_mov_csts] - INFORMACION DE TODAS LAS CUENTAS MASIVO"))
    print(lne_dvs())
    df_tmp_universo_fact_mov_csts=spark.sql(tmp_universo_fact_mov_csts()).cache()
    df_tmp_universo_fact_mov_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_universo_fact_mov_csts.createOrReplaceTempView("tmp_universo_fact_mov_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_universo_fact_mov_csts",str(df_tmp_universo_fact_mov_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_universo_fact_mov_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_ppal_completa_csts")
    spark.catalog.dropTempView("tmp_cuenta_segmento_csts")
    
    vStp01="Paso 42"
    print(lne_dvs())
    print(etq_info("Paso [42]: Ejecucion de funcion [tmp_universo_fact_mov2_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE LINEA NEGOCIO, SEGMENTO, SUBSEGMENTO, BND_LN"))
    print(lne_dvs())
    df_tmp_universo_fact_mov2_csts=spark.sql(tmp_universo_fact_mov2_csts()).cache()
    df_tmp_universo_fact_mov2_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_universo_fact_mov2_csts.createOrReplaceTempView("tmp_universo_fact_mov2_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_universo_fact_mov2_csts",str(df_tmp_universo_fact_mov2_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_universo_fact_mov2_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_universo_fact_mov_csts")
    spark.catalog.dropTempView("tmp_cuenta_seg_masivo_csts")
    
    vStp01="Paso 43"
    print(lne_dvs())
    print(etq_info("Paso [43]: Ejecucion de funcion [tmp_perimetros_unicos_csts] - INFORMACION DE PERIMETROS DEL ANIO Y MES DE ANALISIS"))
    print(lne_dvs())
    df_tmp_perimetros_unicos_csts=spark.sql(tmp_perimetros_unicos_csts(vsolo_anio,vsolo_mes)).cache()
    df_tmp_perimetros_unicos_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_perimetros_unicos_csts.createOrReplaceTempView("tmp_perimetros_unicos_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_perimetros_unicos_csts",str(df_tmp_perimetros_unicos_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_perimetros_unicos_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 44"
    print(lne_dvs())
    print(etq_info("Paso [44]: Ejecucion de funcion [tmp_fact_mov_perimetro_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE CAMPOS Y OBTENCION DE CAMPOS DE PERIMETRO"))
    print(lne_dvs())
    df_tmp_fact_mov_perimetro_csts=spark.sql(tmp_fact_mov_perimetro_csts()).cache()
    df_tmp_fact_mov_perimetro_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_mov_perimetro_csts.createOrReplaceTempView("tmp_fact_mov_perimetro_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_mov_perimetro_csts",str(df_tmp_fact_mov_perimetro_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_mov_perimetro_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_universo_fact_mov2_csts")
    
    
    vStp01="Paso 45"
    print(lne_dvs())
    print(etq_info("Paso [45]: Ejecucion de funcion [tmp_fact_mov_update_csts]- INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE CAMPOS"))
    print(lne_dvs())
    df_tmp_fact_mov_update_csts=spark.sql(tmp_fact_mov_update_csts()).cache()
    df_tmp_fact_mov_update_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_mov_update_csts.createOrReplaceTempView("tmp_fact_mov_update_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_mov_update_csts",str(df_tmp_fact_mov_update_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_mov_update_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_mov_perimetro_csts")
    
    vStp01="Paso 46"
    print(lne_dvs())
    print(etq_info("Paso [46]: Ejecucion de funcion [tmp_mines_planes_csts]- INFORMACION DE LOS MINES CON SUS PLANES"))
    print(lne_dvs())
    df_tmp_mines_planes_csts=spark.sql(tmp_mines_planes_csts()).cache()
    df_tmp_mines_planes_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_mines_planes_csts.createOrReplaceTempView("tmp_mines_planes_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_mines_planes_csts",str(df_tmp_mines_planes_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_mines_planes_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_mov_parque_sin_dup_csts")
    
    #VtablaTmpFactMov="db_desarrollo2021.tmp_fact_mov_final_csts"
    vStp01="Paso 47"
    print(lne_dvs())
    print(etq_info("Paso [47]: Ejecucion de funcion [tmp_fact_mov_final_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON EL MOVIMIENTO FINAL Y ACTUALIZA ALGUNOS CAMPOS"))
    print(lne_dvs())
    df_tmp_fact_mov_final_csts=spark.sql(tmp_fact_mov_final_csts()).cache()
    df_tmp_fact_mov_final_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_mov_final_csts.write.mode("overwrite").format("orc").saveAsTable(VtablaTmpFactMov)
    print(etq_info("Insercion Ok de la tabla temporal: "+str(VtablaTmpFactMov))) 
    print(etq_info(msg_t_total_registros_hive("df_tmp_fact_mov_final_csts",str(df_tmp_fact_mov_final_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_mov_final_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_mov_update_csts")
    spark.catalog.dropTempView("tmp_mines_planes_csts")
    spark.catalog.dropTempView("tmp_mov_parque_antiguo_csts")
    
    vStp01="Paso 48"
    print(lne_dvs())
    print(etq_info("Paso [48]: Ejecucion de funcion [tmp_fact_mov_final_upd_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE ALGUNOS CAMPOS"))
    print(lne_dvs())
    df_tmp_fact_mov_final_upd_csts=spark.sql(tmp_fact_mov_final_upd_csts(VtablaTmpFactMov)).cache()
    df_tmp_fact_mov_final_upd_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_mov_final_upd_csts.createOrReplaceTempView("tmp_fact_mov_final_upd_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_mov_final_upd_csts",str(df_tmp_fact_mov_final_upd_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_mov_final_upd_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_mov_final_csts")
    
    vStp01="Paso 49"
    print(lne_dvs())
    print(etq_info("Paso [49]: Ejecucion de funcion [tmp_fact_mov_final_upd1_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE ALGUNOS CAMPOS"))
    print(lne_dvs())
    df_tmp_fact_mov_final_upd1_csts=spark.sql(tmp_fact_mov_final_upd1_csts()).cache()
    df_tmp_fact_mov_final_upd1_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_mov_final_upd1_csts.createOrReplaceTempView("tmp_fact_mov_final_upd1_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_mov_final_upd1_csts",str(df_tmp_fact_mov_final_upd1_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_mov_final_upd1_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_mov_final_upd_csts")
    
    vStp01="Paso 50"
    print(lne_dvs())
    print(etq_info("Paso [50]: Ejecucion de funcion [tmp_fact_mov_final_costo_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON COSTOS"))
    print(lne_dvs())
    df_tmp_fact_mov_final_costo_csts=spark.sql(tmp_fact_mov_final_costo_csts()).cache()
    df_tmp_fact_mov_final_costo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_mov_final_costo_csts.createOrReplaceTempView("tmp_fact_mov_final_costo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_mov_final_costo_csts",str(df_tmp_fact_mov_final_costo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_mov_final_costo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_imei_articulo_csts")
    spark.catalog.dropTempView("tmp_fact_mov_final_upd1_csts")
    
    vStp01="Paso 51"
    print(lne_dvs())
    print(etq_info("Paso [51]: Ejecucion de funcion [tmp_costo_x_modelo_csts] - INFORMACION DE COSTO POR MODELO Y FUENTE SIN DUPLICADOS"))
    print(lne_dvs())
    df_tmp_costo_x_modelo_csts=spark.sql(tmp_costo_x_modelo_csts()).cache()
    df_tmp_costo_x_modelo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_x_modelo_csts.createOrReplaceTempView("tmp_costo_x_modelo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_x_modelo_csts",str(df_tmp_costo_x_modelo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_x_modelo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 52"
    print(lne_dvs())
    print(etq_info("Paso [52]: Ejecucion de funcion [tmp_costo_sin_imei_csts] - INFORMACION DE COSTO POR MODELO SIN IMEI"))
    print(lne_dvs())
    df_tmp_costo_sin_imei_csts=spark.sql(tmp_costo_sin_imei_csts()).cache()
    df_tmp_costo_sin_imei_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_sin_imei_csts.createOrReplaceTempView("tmp_costo_sin_imei_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_sin_imei_csts",str(df_tmp_costo_sin_imei_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_sin_imei_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_mov_final_costo_csts")
    spark.catalog.dropTempView("tmp_costo_x_modelo_csts")
    
    vStp01="Paso 53"
    print(lne_dvs())
    print(etq_info("Paso [53]: Ejecucion de funcion [tmp_costo_rep_anterior_csts] - INFORMACION DE COSTO POR MODELO DEL MES ANTERIOR"))
    print(lne_dvs())
    df_tmp_costo_rep_anterior_csts=spark.sql(tmp_costo_rep_anterior_csts(vfecha_meses_atras1,vfecha_inicio)).cache()
    df_tmp_costo_rep_anterior_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_rep_anterior_csts.createOrReplaceTempView("tmp_costo_rep_anterior_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_rep_anterior_csts",str(df_tmp_costo_rep_anterior_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_rep_anterior_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    #VtablaTmpCostFactFin="db_desarrollo2021.tmp_costo_fac_final_csts"
    vStp01="Paso 54"
    print(lne_dvs())
    print(etq_info("Paso [54]: Ejecucion de funcion [tmp_costo_fac_final_csts] - INFORMACION DE COSTO FINAL"))
    print(lne_dvs())
    df_tmp_costo_fac_final_csts=spark.sql(tmp_costo_fac_final_csts(vultimo_dia_act_frmt)).cache()
    df_tmp_costo_fac_final_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fac_final_csts.write.mode("overwrite").format("orc").saveAsTable(VtablaTmpCostFactFin)
    print(etq_info("Insercion Ok de la tabla temporal: "+str(VtablaTmpCostFactFin))) 
    print(etq_info(msg_t_total_registros_hive("df_tmp_costo_fac_final_csts",str(df_tmp_costo_fac_final_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fac_final_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_sin_imei_csts")
    
    vStp01="Paso 55"
    print(lne_dvs())
    print(etq_info("Paso [55]: Ejecucion de funcion [tmp_cuota_monto_csts] - INFORMACION DE CUOTA INICIAL Y MONTO FINANCIADO"))
    print(lne_dvs())
    df_tmp_cuota_monto_csts=spark.sql(tmp_cuota_monto_csts(vfecha_inicio,vfecha_fin)).cache()
    df_tmp_cuota_monto_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_cuota_monto_csts.createOrReplaceTempView("tmp_cuota_monto_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_cuota_monto_csts",str(df_tmp_cuota_monto_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_cuota_monto_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 56"
    print(lne_dvs())
    print(etq_info("Paso [56]: Ejecucion de funcion [tmp_cuotas_financiadas_csts] - INFORMACION DEL NUMERO DE CUOTAS FINANCIADAS"))
    print(lne_dvs())
    df_tmp_cuotas_financiadas_csts=spark.sql(tmp_cuotas_financiadas_csts()).cache()
    df_tmp_cuotas_financiadas_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_cuotas_financiadas_csts.createOrReplaceTempView("tmp_cuotas_financiadas_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_cuotas_financiadas_csts",str(df_tmp_cuotas_financiadas_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_cuotas_financiadas_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 57"
    print(lne_dvs())
    print(etq_info("Paso [57]: Ejecucion de funcion [tmp_concepto_articulo_csts] - INFORMACION DE LA RELACION CONCEPTO ARTICULO"))
    print(lne_dvs())
    df_tmp_concepto_articulo_csts=spark.sql(tmp_concepto_articulo_csts(VtablaTmpCostFactFin)).cache()
    df_tmp_concepto_articulo_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_concepto_articulo_csts.createOrReplaceTempView("tmp_concepto_articulo_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_concepto_articulo_csts",str(df_tmp_concepto_articulo_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_concepto_articulo_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 58"
    print(lne_dvs())
    print(etq_info("Paso [58]: Ejecucion de funcion [tmp_billsummary_billseq_csts] - INFORMACION DE billsummary PARA OBTENER TODAS LAS SECUENCIAS"))
    print(lne_dvs())
    df_tmp_billsummary_billseq_csts=spark.sql(tmp_billsummary_billseq_csts(vfecha_inicio,vfecha_fin)).cache()
    df_tmp_billsummary_billseq_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_billsummary_billseq_csts.createOrReplaceTempView("tmp_billsummary_billseq_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_billsummary_billseq_csts",str(df_tmp_billsummary_billseq_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_billsummary_billseq_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 59"
    print(lne_dvs())
    print(etq_info("Paso [59]: Ejecucion de funcion [tmp_fact_final_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON LAS CUOTAS, CANAL Y USUARIOS"))
    print(lne_dvs())
    df_tmp_fact_final_csts=spark.sql(tmp_fact_final_csts(VtablaTmpCostFactFin)).cache()
    df_tmp_fact_final_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_csts.createOrReplaceTempView("tmp_fact_final_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_csts",str(df_tmp_fact_final_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_billsummary_billseq_csts")
    spark.catalog.dropTempView("tmp_cuotas_financiadas_csts")
    spark.catalog.dropTempView("tmp_costo_rep_anterior_csts")
    spark.catalog.dropTempView("tmp_costo_sin_imei_csts")
    
    vStp01="Paso 60"
    print(lne_dvs())
    print(etq_info("Paso [60]: Ejecucion de funcion [tmp_fact_final_tipcanal_csts] - INFORMACION DEL UNIVERSO PRINCIPAL CON EL TIPO DE CANAL"))
    print(lne_dvs())
    df_tmp_fact_final_tipcanal_csts=spark.sql(tmp_fact_final_tipcanal_csts()).cache()
    df_tmp_fact_final_tipcanal_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_tipcanal_csts.createOrReplaceTempView("tmp_fact_final_tipcanal_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_tipcanal_csts",str(df_tmp_fact_final_tipcanal_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_tipcanal_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_final_csts")
    
    vStp01="Paso 61"
    print(lne_dvs())
    print(etq_info("Paso [61]: Ejecucion de funcion [tmp_campos_para_nc_csts] - INFORMACION DE CAMPOS PARA NOTAS DE CREDITO"))
    print(lne_dvs())
    df_tmp_campos_para_nc_csts=spark.sql(tmp_campos_para_nc_csts(vfecha_meses_atras2,vfecha_fin)).cache()
    df_tmp_campos_para_nc_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_campos_para_nc_csts.createOrReplaceTempView("tmp_campos_para_nc_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_campos_para_nc_csts",str(df_tmp_campos_para_nc_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_campos_para_nc_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    #
    
    vStp01="Paso 62"
    print(lne_dvs())
    print(etq_info("Paso [62]: Ejecucion de funcion [tmp_costos_fact_final_v2_csts] - UNIVERSO PRINCIPAL CON LOS CAMPOS DE NOTAS DE CREDITO"))
    print(lne_dvs())
    df_tmp_costos_fact_final_v2_csts=spark.sql(tmp_costos_fact_final_v2_csts()).cache()
    df_tmp_costos_fact_final_v2_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costos_fact_final_v2_csts.createOrReplaceTempView("tmp_costos_fact_final_v2_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costos_fact_final_v2_csts",str(df_tmp_costos_fact_final_v2_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costos_fact_final_v2_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_campos_para_nc_csts")
    spark.catalog.dropTempView("tmp_fact_final_tipcanal_csts")
    
    vStp01="Paso 63"
    print(lne_dvs())
    print(etq_info("Paso [63]: Ejecucion de funcion [tmp_fact_final_update1_csts] - UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS CANAL, REGION, FUENTE CANAL"))
    print(lne_dvs())
    df_tmp_fact_final_update1_csts=spark.sql(tmp_fact_final_update1_csts(vval_usuario4)).cache()
    df_tmp_fact_final_update1_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_update1_csts.createOrReplaceTempView("tmp_fact_final_update1_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_update1_csts",str(df_tmp_fact_final_update1_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_update1_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costos_fact_final_v2_csts")
    
    vStp01="Paso 64"
    print(lne_dvs())
    print(etq_info("Paso [64]: Ejecucion de funcion [tmp_fact_final_update2_csts] - UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS CANAL Y FUENTE CANAL"))
    print(lne_dvs())
    df_tmp_fact_final_update2_csts=spark.sql(tmp_fact_final_update2_csts()).cache()
    df_tmp_fact_final_update2_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_update2_csts.createOrReplaceTempView("tmp_fact_final_update2_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_update2_csts",str(df_tmp_fact_final_update2_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_update2_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_final_update1_csts")
    
    vStp01="Paso 65"
    print(lne_dvs())
    print(etq_info("Paso [65]: Ejecucion de funcion [tmp_fact_final_update3_csts] - UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS LINEA NEGOCIO, SEGMENTO, SUBSEGMENTO Y MOVIMIENTO"))
    print(lne_dvs())
    df_tmp_fact_final_update3_csts=spark.sql(tmp_fact_final_update3_csts()).cache()
    df_tmp_fact_final_update3_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_update3_csts.createOrReplaceTempView("tmp_fact_final_update3_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_update3_csts",str(df_tmp_fact_final_update3_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_update3_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_final_update2_csts")
    
    vStp01="Paso 66"
    print(lne_dvs())
    print(etq_info("Paso [66]: Ejecucion de funcion [tmp_fact_final_update4_csts] - UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS LINEA NEGOCIO, SEGMENTO, SUBSEGMENTO Y MOVIMIENTO"))
    print(lne_dvs())
    df_tmp_fact_final_update4_csts=spark.sql(tmp_fact_final_update4_csts()).cache()
    df_tmp_fact_final_update4_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_update4_csts.createOrReplaceTempView("tmp_fact_final_update4_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_update4_csts",str(df_tmp_fact_final_update4_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_update4_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_final_update3_csts")
    
    vStp01="Paso 67"
    print(lne_dvs())
    print(etq_info("Paso [67]: Ejecucion de funcion [tmp_fact_final_update5_csts] - UNIVERSO PRINCIPAL CON UPDATE DE LOS CAMPOS REGION, SEGMENTO, MOVIMIENTO, FUENTE MOVIMIENTO"))
    print(lne_dvs())
    df_tmp_fact_final_update5_csts=spark.sql(tmp_fact_final_update5_csts()).cache()
    df_tmp_fact_final_update5_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_update5_csts.createOrReplaceTempView("tmp_fact_final_update5_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_update5_csts",str(df_tmp_fact_final_update5_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_update5_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_final_update4_csts")
    
    vStp01="Paso 68"
    print(lne_dvs())
    print(etq_info("Paso [68]: Ejecucion de funcion [tmp_fact_final_update6_csts] - UNIVERSO PRINCIPAL CON UPDATE DE LOS CAMPOS REGION, MOVIMIENTO, FUENTE MOVIMIENTO"))
    print(lne_dvs())
    df_tmp_fact_final_update6_csts=spark.sql(tmp_fact_final_update6_csts()).cache()
    df_tmp_fact_final_update6_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_final_update6_csts.createOrReplaceTempView("tmp_fact_final_update6_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_final_update6_csts",str(df_tmp_fact_final_update6_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_final_update6_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_final_update5_csts")
    
    #vTablaCostFinV2="db_desarrollo2021.tmp_costo_fact_final_v2_1_csts"
    vStp01="Paso 69"
    print(lne_dvs())
    print(etq_info("Paso [69]: Ejecucion de funcion [tmp_costo_fact_final_v2_1_csts] - UNIVERSO PRINCIPAL DESCARTANDO ALGUNOS REGISTROS"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v2_1_csts=spark.sql(tmp_costo_fact_final_v2_1_csts(vval_usuario_final)).cache()
    df_tmp_costo_fact_final_v2_1_csts.printSchema()
    ts_step_tbl = datetime.now()
    #df_tmp_costo_fact_final_v2_1_csts.createOrReplaceTempView("tmp_costo_fact_final_v2_1_csts")
    df_tmp_costo_fact_final_v2_1_csts.write.mode("overwrite").format("orc").saveAsTable(vTablaCostFinV2)
    print(etq_info("Insercion Ok de la tabla temporal: "+str(vTablaCostFinV2))) 
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v2_1_csts",str(df_tmp_costo_fact_final_v2_1_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v2_1_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_fact_final_update6_csts")
    
    vStp01="Paso 70"
    print(lne_dvs())
    print(etq_info("Paso [70]: Ejecucion de funcion [tmp_costo_fact_final_v3_csts] - UNIVERSO PRINCIPAL ACTUALIZANDO CAMPOS A PARTIR DEL CATALOGO TIPO CANAL Y LA INFORMACION DL ARTICULO"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v3_csts=spark.sql(tmp_costo_fact_final_v3_csts(vTablaCostFinV2)).cache()
    df_tmp_costo_fact_final_v3_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_final_v3_csts.createOrReplaceTempView("tmp_costo_fact_final_v3_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v3_csts",str(df_tmp_costo_fact_final_v3_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v3_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_final_v2_1_csts")
    
    vStp01="Paso [71]"
    print(lne_dvs())
    print(etq_info(vStp01+": Ejecucion de funcion [tmp_costo_fact_final_v4_csts] - UNIVERSO PRINCIPAL ACTUALIZANDO ALGUNOS CAMPOS"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v4_csts=spark.sql(tmp_costo_fact_final_v4_csts(vval_usuario4)).cache()
    df_tmp_costo_fact_final_v4_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_final_v4_csts.createOrReplaceTempView("tmp_costo_fact_final_v4_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v4_csts",str(df_tmp_costo_fact_final_v4_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v4_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_usuario_cm_csts")
    spark.catalog.dropTempView("tmp_costo_fact_final_v3_csts")
    spark.catalog.dropTempView("tmp_concepto_articulo_csts")
    
    vStp01="Paso [72]"
    print(lne_dvs())
    print(etq_info(vStp01+": Ejecucion de funcion [tmp_costo_fact_final_v4up_csts] - UNIVERSO PRINCIPAL ACTUALIZANDO LOS CAMPOS DE REGION, FUENTE CANAL Y CANAL"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v4up_csts=spark.sql(tmp_costo_fact_final_v4up_csts(vval_usuario4)).cache()
    df_tmp_costo_fact_final_v4up_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_final_v4up_csts.createOrReplaceTempView("tmp_costo_fact_final_v4up_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v4up_csts",str(df_tmp_costo_fact_final_v4up_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v4up_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_final_v4_csts")
    
    vStp01="Paso [73]"
    print(lne_dvs())
    print(etq_info(vStp01+": Ejecucion de funcion [tmp_costo_fact_final_v5up_csts] - UNIVERSO PRINCIPAL ACTUALIZANDO LOS CAMPOS DE REGION, CANAL, SEGMENTO Y TIPO VENTA"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v5up_csts=spark.sql(tmp_costo_fact_final_v5up_csts()).cache()
    df_tmp_costo_fact_final_v5up_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_final_v5up_csts.createOrReplaceTempView("tmp_costo_fact_final_v5up_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v5up_csts",str(df_tmp_costo_fact_final_v5up_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v5up_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_final_v4up_csts")
    
    vStp01="Paso [74]"
    print(lne_dvs())
    print(etq_info(vStp01+": Ejecucion de funcion [tmp_costo_fact_final_v5_csts] -  UNIVERSO PRINCIPAL ACTUALIZANDO LOS CAMPOS REGION Y CANAL"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v5_csts=spark.sql(tmp_costo_fact_final_v5_csts()).cache()
    df_tmp_costo_fact_final_v5_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_final_v5_csts.createOrReplaceTempView("tmp_costo_fact_final_v5_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v5_csts",str(df_tmp_costo_fact_final_v5_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v5_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_final_v5up_csts")
    
    vStp01="Paso [75]"
    print(lne_dvs())
    print(etq_info(vStp01+": Ejecucion de funcion [tmp_tarjeta_banco_csts] - INFORMACION DE LA TARJETA BANCO"))
    print(lne_dvs())
    df_tmp_tarjeta_banco_csts=spark.sql(tmp_tarjeta_banco_csts(vfecha_inicio,vfecha_fin)).cache()
    df_tmp_tarjeta_banco_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_tarjeta_banco_csts.createOrReplaceTempView("tmp_tarjeta_banco_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_tarjeta_banco_csts",str(df_tmp_tarjeta_banco_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_tarjeta_banco_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    
    vStp01="Paso 76"
    print(lne_dvs())
    print(etq_info("Paso [76]: Ejecucion de funcion [tmp_tarjeta_banco1_csts] - INFORMACION DE LA TARJETA DE PAGO"))
    print(lne_dvs())
    df_tmp_tarjeta_banco1_csts=spark.sql(tmp_tarjeta_banco1_csts()).cache()
    df_tmp_tarjeta_banco1_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_tarjeta_banco1_csts.createOrReplaceTempView("tmp_tarjeta_banco1_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_tarjeta_banco1_csts",str(df_tmp_tarjeta_banco1_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_tarjeta_banco1_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 77"
    print(lne_dvs())
    print(etq_info("Paso [77]: Ejecucion de funcion [tmp_pivot_tarjeta_banco_csts]-  PIVOT DE LA INFORMACION DE LA TARJETA BANCO"))
    print(lne_dvs())
    df_tmp_pivot_tarjeta_banco_csts=spark.sql(tmp_pivot_tarjeta_banco_csts()).cache()
    df_tmp_pivot_tarjeta_banco_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_pivot_tarjeta_banco_csts.createOrReplaceTempView("tmp_pivot_tarjeta_banco_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_pivot_tarjeta_banco_csts",str(df_tmp_pivot_tarjeta_banco_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_pivot_tarjeta_banco_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_tarjeta_banco1_csts")
    
    vStp01="Paso 78"
    print(lne_dvs())
    print(etq_info("Paso [78]: Ejecucion de funcion [tmp_costo_fact_final_v6_csts] - UNIVERSO PRINCIPAL CON LA INFORMACION DEL MONTO FINANCIADO SIN IVA Y LA TARJETA BANCO"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v6_csts=spark.sql(tmp_costo_fact_final_v6_csts()).cache()
    df_tmp_costo_fact_final_v6_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_final_v6_csts.createOrReplaceTempView("tmp_costo_fact_final_v6_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v6_csts",str(df_tmp_costo_fact_final_v6_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v6_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_final_v5_csts")
    spark.catalog.dropTempView("tmp_pivot_tarjeta_banco_csts")
    spark.catalog.dropTempView("tmp_cuota_monto_csts")
    
    vStp01="Paso 79"
    print(lne_dvs())
    print(etq_info("Paso [79]: Ejecucion de funcion [tmp_costo_fact_final_v4_1_csts] - UNIVERSO PRINCIPAL CON CALCULO DE ALGUNOS CAMPOS Y ASIGNACION DE MONTOS Y TARJETA"))
    print(lne_dvs())
    df_tmp_costo_fact_final_v4_1_csts=spark.sql(tmp_costo_fact_final_v4_1_csts()).cache()
    df_tmp_costo_fact_final_v4_1_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_final_v4_1_csts.createOrReplaceTempView("tmp_costo_fact_final_v4_1_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_final_v4_1_csts",str(df_tmp_costo_fact_final_v4_1_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_final_v4_1_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_final_v6_csts")
    spark.catalog.dropTempView("tmp_tarjeta_banco_csts")
    
    vStp01="Paso 80"
    print(lne_dvs())
    print(etq_info("Paso [80]: Ejecucion de funcion [tmp_costo_fact_exporta_csts] - UNIVERSO PRINCIPAL CON ACTUALIZACION EN ALGUNOS CAMPOS"))
    print(lne_dvs())
    df_tmp_costo_fact_exporta_csts=spark.sql(tmp_costo_fact_exporta_csts(vfecha_inicio,vfecha_fin,vfecha_antes_ayer)).cache()
    df_tmp_costo_fact_exporta_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_exporta_csts.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_termi_80')
    df_tmp_costo_fact_exporta_csts.createOrReplaceTempView("tmp_costo_fact_exporta_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_exporta_csts",str(df_tmp_costo_fact_exporta_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_exporta_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_final_v4_1_csts")
    spark.catalog.dropTempView("tmp_perimetros_unicos_csts")
    
    vStp01="Paso 81"
    print(lne_dvs())
    print(etq_info("Paso [81]: Ejecucion de funcion [tmp_costo_fact_exporta_otra_csts] - UNIVERSO PRINCIPAL CON ACTUALIZACION DE CAMPOS PARA CUADRES"))
    print(lne_dvs())
    df_tmp_costo_fact_exporta_otra_csts=spark.sql(tmp_costo_fact_exporta_otra_csts()).cache()
    df_tmp_costo_fact_exporta_otra_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_exporta_otra_csts.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_termi_81')
    df_tmp_costo_fact_exporta_otra_csts.createOrReplaceTempView("tmp_costo_fact_exporta_otra_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_exporta_otra_csts",str(df_tmp_costo_fact_exporta_otra_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_exporta_otra_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_exporta_csts")
    
    vStp01="Paso 82"
    print(lne_dvs())
    print(etq_info("Paso [82]: Ejecucion de funcion [tmp_costo_fact_exporta_otra1_csts] - UNIVERSO PRINCIPAL CON IDs"))
    print(lne_dvs())
    df_tmp_costo_fact_exporta_otra1_csts=spark.sql(tmp_costo_fact_exporta_otra1_csts()).cache()
    df_tmp_costo_fact_exporta_otra1_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_costo_fact_exporta_otra1_csts.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_termi_82')
    df_tmp_costo_fact_exporta_otra1_csts.createOrReplaceTempView("tmp_costo_fact_exporta_otra1_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_costo_fact_exporta_otra1_csts",str(df_tmp_costo_fact_exporta_otra1_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_costo_fact_exporta_otra1_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_exporta_otra_csts")
    
    vStp01="Paso 83"
    print(lne_dvs())
    print(etq_info("Paso [83]: Ejecucion de funcion [tmp_fact_exporta_nodupli_csts] - UNIVERSO PRINCIPAL SIN DUPLICADOS"))
    print(lne_dvs())
    df_tmp_fact_exporta_nodupli_csts=spark.sql(tmp_fact_exporta_nodupli_csts(vfecha_antes_ayer)).cache()
    df_tmp_fact_exporta_nodupli_csts.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_fact_exporta_nodupli_csts.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_termi_83')
    df_tmp_fact_exporta_nodupli_csts.createOrReplaceTempView("tmp_fact_exporta_nodupli_csts")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_fact_exporta_nodupli_csts",str(df_tmp_fact_exporta_nodupli_csts.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_fact_exporta_nodupli_csts",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_costo_fact_exporta_otra1_csts")
    
    vStp01="Paso 84"
    print(lne_dvs())
    print(etq_info("Paso [84]: Ejecucion de funcion [otc_t_terminales_simcards] - INFORMACION DE TODAS LAS PARTICIONES CORRESPONDIENTES AL MES DE PROCESO (DESDE EL DIA 1 HASTA DIA CAIDO)"))
    print(lne_dvs())
    df_otc_t_terminales_simcards=spark.sql(otc_t_terminales_simcards(vanio_mes)).cache()
    df_otc_t_terminales_simcards.printSchema()
    ts_step_tbl = datetime.now()
    columns = spark.table(vBaseHive+"."+vTablaDestino).columns
    cols = []
    for column in columns:
        cols.append(column)
    df_otc_t_terminales_simcards = df_otc_t_terminales_simcards.select(cols)
    
    print(etq_info("REALIZA EL BORRADO DE PARTICIONES CORRESPONDIENTES AL MES DE PROCESO (DESDE EL DIA 1 HASTA DIA CAIDO)"))
    partitions = spark.sql("SHOW PARTITIONS "+vBaseHive+"."+vTablaDestino)
    listpartitions = list(partitions.select('partition').toPandas()['partition'])
    cleanpartitions = [ i.split('=')[1] for i in listpartitions]
    filtered = [i for i in cleanpartitions if i >= str(vfecha_inicio) and i < str(vfecha_fin)]
    for i in filtered:
        query_truncate = "ALTER TABLE "+vBaseHive+"."+vTablaDestino+" DROP IF EXISTS PARTITION (p_fecha_factura = " + str(i)+ ") purge"
        print(query_truncate)
        spark.sql(query_truncate)
    
    df_otc_t_terminales_simcards.write.mode("append").insertInto(vBaseHive+"."+vTablaDestino)
    print(etq_info("Insercion Ok de la tabla destino: "+str(vTablaDestino))) 
    print(etq_info(msg_t_total_registros_hive("df_otc_t_terminales_simcards",str(df_otc_t_terminales_simcards.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_otc_t_terminales_simcards",vle_duracion(ts_step_tbl,te_step_tbl))))
    
##**************************************************************--
##******  Cambio de alcance (2023-04-12) en Spark (Cristian Ortiz) ****--
##**************************************************************--
    vStp01="Paso 85"
    print(lne_dvs())
    print(etq_info("Paso [85]: Ejecucion de funcion [tmp_terminales_simcards_nc] - Tabla Temporal con terminales de notas de credito"))
    print(lne_dvs())
    df85=spark.sql(tmp_terminales_simcards_nc(vultimo_dia_act_frmt,vfecha_inicio,vfecha_fin)).cache()
    df85.printSchema()
    ts_step_tbl = datetime.now()
    df85.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_terminales_simcards_nc')
    print(etq_info(msg_t_total_registros_obtenidos("df85",str(df85.count())))) #BORRAR
    te_step_tbl = datetime.now()
    del df85
    print(etq_info(msg_d_duracion_hive("df85",vle_duracion(ts_step_tbl,te_step_tbl))))

    vStp01="Paso 86"
    print(lne_dvs())
    print(etq_info("Paso [86]: Ejecucion de funcion [tmp_terminales_simcards_factura] - Tabla Temporal de terminales con factura, es decir diferentes de notas de credito"))
    print(lne_dvs())
    df86=spark.sql(tmp_terminales_simcards_factura(vultimo_dia_act_frmt,vfecha_inicio,vfecha_fin)).cache()
    df86.printSchema()
    ts_step_tbl = datetime.now()
    df86.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_terminales_simcards_factura')
    print(etq_info(msg_t_total_registros_obtenidos("df86",str(df86.count())))) #BORRAR
    te_step_tbl = datetime.now()
    del df86
    print(etq_info(msg_d_duracion_hive("df86",vle_duracion(ts_step_tbl,te_step_tbl))))

    vStp01="Paso 87"
    print(lne_dvs())
    print(etq_info("Paso [87]: Ejecucion de funcion [tmp_terminales_simcards] - TABLA con la union de terminales NC y facturas"))
    print(lne_dvs())
    df87=spark.sql(tmp_terminales_simcards(vfecha_antes_ayer)).cache()
    df87.printSchema()
    ts_step_tbl = datetime.now()
    df87.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_terminales_simcards')
    print(etq_info(msg_t_total_registros_obtenidos("df87",str(df87.count())))) #BORRAR
    te_step_tbl = datetime.now()
    del df87
    print(etq_info(msg_d_duracion_hive("df87",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 88"
    print(lne_dvs())
    print(etq_info("Paso [88]: Ejecucion de funcion [otc_t_ext_terminales_ajst] - TABLA FINAL PARA REPORTE DE EXTRACTOR DE TERMINALES"))
    print(lne_dvs())
    df88=spark.sql(otc_t_ext_terminales_ajst()).cache()
    df88.printSchema()
    ts_step_tbl = datetime.now()
    df88.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.otc_t_ext_terminales_ajst')
    print(etq_info(msg_t_total_registros_obtenidos("df88",str(df88.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df88",vle_duracion(ts_step_tbl,te_step_tbl))))
    
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df88
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(msg_e_ejecucion(vStpFin,str(e)))

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())

