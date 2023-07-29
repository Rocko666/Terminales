set -e
#!/bin/bash
#########################################################################################################
# NOMBRE: OTC_T_TERMINALES_FACT.sh  			      										            #
# DESCRIPCION:																							#
#   Shell sqoop encargado de extraer la informacion de la tabla OTC_T_TERMINALES_FACT de la fuente RBM	#
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2022-07-07   																			#
# PARAMETROS DEL SHELL                            													    #
# VAL_FECHAEJE			Fecha de ejecucion del proceso YYYYMMDD											#
#########################################################################################################
# MODIFICACIONES														 								#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO						 								#
# 2022-12-29	Brigitte Balon	Se migra importacion a spark			 								#													 								#
# 2023-07-27	Cristian Ortiz	BIGD-62 cambio a ejecucion diaria y tabla final particionada            #								
#########################################################################################################

ENTIDAD=TRMNLSFCT0040
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

VAL_FECHAEJE=$1

#PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_NOM_JAR_ORC_11=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_11';"`
TDUSER_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDUSER_RBM';"`
TDPASS_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDPASS_RBM';"`
TDHOST_RBM2=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDHOST_RBM2';"`
TDPORT_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDPORT_RBM';"`
TDSERVICE_RBM1=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDSERVICE_RBM1';"`
TDCLASS_ORC=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDCLASS_ORC';"`

#PARAMETROS PROPIOS DEL PROCESO OBTENIDOS DE LA TABLA params
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`
VTTEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VTTEMP';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`

#PARAMETROS DE OPERACION Y AUTOGENERADOS
VAL_FEC_AYER=`date -d "${VAL_FECHAEJE} -1 day"  +"%Y%m%d"`
VAL_FEC_FIN=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m01"`
VAL_FEC_INI=`date -d "${VAL_FEC_FIN} -1 day"  +"%Y%m01"`
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/OTC_T_TERMINALES_FACT_$VAL_DIA$VAL_HORA.log
VAL_JDBCURL=jdbc:oracle:thin:@//$TDHOST_RBM2:$TDPORT_RBM/$TDSERVICE_RBM1

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_RUTA_SPARK" ] || 
    [ -z "$VAL_RUTA_LIB" ] || 
    [ -z "$VAL_NOM_JAR_ORC_11" ] || 
    [ -z "$TDHOST_RBM2" ] || 
    [ -z "$TDPASS_RBM" ] || 
    [ -z "$TDPORT_RBM" ] || 
    [ -z "$TDSERVICE_RBM1" ] || 
    [ -z "$TDCLASS_ORC" ] || 
    [ -z "$TDUSER_RBM" ] ||
    [ -z "$VAL_MASTER" ] || 
    [ -z "$VAL_DRIVER_MEMORY" ] || 
    [ -z "$VAL_EXECUTOR_MEMORY" ] || 
    [ -z "$VAL_NUM_EXECUTORS" ] || 
    [ -z "$VAL_NUM_EXECUTORS_CORES" ] ||  
    [ -z "$VAL_RUTA" ] || 
    [ -z "$HIVETABLE" ] || 
    [ -z "$VTTEMP" ] || 
    [ -z "$HIVEDB" ] || 
    [ -z "$VAL_TIPO_CARGA" ] || 
    [ -z "$VAL_JDBCURL" ] || 
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== Ejecuta el proceso - Fuente RBM - OTC_T_TERMINALES_FACT ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: $VAL_RUTA/otc_t_terminales_fact.py " 2>&1 &>> $VAL_LOG
echo "Fecha Inicio: $VAL_FEC_FIN " 2>&1 &>> $VAL_LOG
echo "Fecha Fin: $VAL_FEC_AYER " 2>&1 &>> $VAL_LOG
echo "Tabla destino: $HIVEDB.$HIVETABLE" 2>&1 &>> $VAL_LOG
echo "Usuario BD: $TDUSER_RBM" 2>&1 &>> $VAL_LOG
echo "Password BD: $TDPASS_RBM" 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--master $VAL_MASTER \
--queue capa_semantica \
--name OTC_T_TERMINALES_FACT \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA/python/otc_t_terminales_fact.py \
--vclass=$TDCLASS_ORC \
--vjdbcurl=$VAL_JDBCURL \
--vusuariobd=$TDUSER_RBM \
--vclavebd=$TDPASS_RBM \
--vhivebd=$HIVEDB \
--vtablahive=$HIVETABLE \
--vtipocarga=$VAL_TIPO_CARGA \
--vfechai=$VAL_FEC_FIN \
--vfechaf=$VAL_FEC_AYER \
--vttemp=$VTTEMP \
--vcampoparte="pt_fecha" 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'invalid syntax|Traceback|An error occurred|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark otc_t_terminales_fact.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark otc_t_terminales_fact.py ====" 2>&1 &>> $VAL_LOG
exit 1
fi

echo "==== Finaliza ejecucion del proceso OTC_T_TERMINALES_FACT ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
