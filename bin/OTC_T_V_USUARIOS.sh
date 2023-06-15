set -e
#!/bin/bash
#########################################################################################################
# NOMBRE: OTC_T_V_USUARIOS.sh     				      											        #
# DESCRIPCION:																							#
#   Realiza el proceso de importacion de RDB Oracle a Hive, tabla otc_v_usuarios			            #
# AUTOR: Karina Castro                           														#
# FECHA CREACION: 2022-07-11   																			#
# PARAMETROS DEL SHELL                            													    #
# N/A					   													                            #
#########################################################################################################
# MODIFICACIONES														 								#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO						 								#
# 2022-12-29	Brigitte Balon	Se migra importacion a spark			 								#								
#########################################################################################################

ENTIDAD=D_URMTRMNLSVUSR3090

#PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params_des
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_NOM_JAR_ORC_11=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_11';"`
VAL_RUTA_IMP_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_IMP_SPARK';"`
VAL_NOM_IMP_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_NOM_IMP_SPARK';"`
TDUSER_RDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'TDUSER_RDB';"`
TDPASS_RDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'TDPASS_RDB';"`
TDHOST_RDB2=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'TDHOST_RDB2';"`
TDPORT_RDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'TDPORT_RDB';"`
TDSERVICE_RDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'TDSERVICE_RDB';"`

#PARAMETROS PROPIOS DEL PROCESO OBTENIDOS DE LA TABLA params_des
VAL_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
TDTABLE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDTABLE';"`
HIVEDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_MASTER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`


#PARAMETROS DE OPERACION Y AUTOGENERADOS
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/logs/OTC_T_V_USUARIOS_$VAL_DIA$VAL_HORA.log
VAL_JDBCURL=jdbc:oracle:thin:@//$TDHOST_RDB2:$TDPORT_RDB/$TDSERVICE_RDB
HIVETABLE="otc_t_v_usuarios"

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_RUTA_SPARK" ] || 
    [ -z "$VAL_RUTA_LIB" ] || 
    [ -z "$VAL_NOM_JAR_ORC_11" ] || 
    [ -z "$TDHOST_RDB2" ] || 
    [ -z "$TDPASS_RDB" ] || 
    [ -z "$TDPORT_RDB" ] || 
    [ -z "$TDSERVICE_RDB" ] || 
    [ -z "$TDUSER_RDB" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$TDTABLE" ] || 
    [ -z "$HIVEDB" ] || 
    [ -z "$VAL_TIPO_CARGA" ] || 
    [ -z "$VAL_JDBCURL" ] || 
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== Ejecuta el proceso - Fuente RDB - OTC_V_USUARIOS ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: $VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK " 2>&1 &>> $VAL_LOG
echo "Tabla destino: $HIVEDB.$HIVETABLE" 2>&1 &>> $VAL_LOG
echo "Usuario BD: $TDUSER_RDB" 2>&1 &>> $VAL_LOG
echo "Password BD: $TDPASS_RDB" 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.port.maxRetries=100 \
--master $VAL_MASTER \
--name OTC_V_USUARIOS \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL \
--vusuariobd=$TDUSER_RDB \
--vclavebd=$TDPASS_RDB \
--vhivebd=$HIVEDB \
--vtablahive=$HIVETABLE \
--vtipocarga=$VAL_TIPO_CARGA \
--vfilesql=$VAL_RUTA/python/otc_t_v_usuarios.sql 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'invalid syntax|Traceback|An error occurred|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark $VAL_NOM_IMP_SPARK es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark $VAL_NOM_IMP_SPARK ====" 2>&1 &>> $VAL_LOG
exit 1
fi

echo "==== Finaliza ejecucion del proceso OTC_V_USUARIOS ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
