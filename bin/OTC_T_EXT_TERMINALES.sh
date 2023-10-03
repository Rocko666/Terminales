set -e
#!/bin/bash
#########################################################################################################
# NOMBRE: OTC_T_EXT_TERMINALES.sh   	     	      								         		    #
# DESCRIPCION:																							#
# Shell que realiza el proceso mensual para generar reporte Extractor_Terminales.txt					#
# Script de carga de archivo AJUSTES_TERMINALES.xlsx desde SFTP para subirlo a como tabla a HIVE			#
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2021-08-25   																			#
# PARAMETROS DEL SHELL                            													    #
# VAL_FECHA_EJEC=${1} 		Fecha de ejecucion de proceso en formato  YYYYMMDD                          #
# VAL_RUTA=${2} 			Ruta donde se encuentran los objetos del proceso                            #
#########################################################################################################
# MODIFICACIONES														 								#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO						 								#
# 2022-12-30	Brigitte Balon	Se migra importacion a spark			 								#
# 2023-07-14    Cristian Ortiz	Extractor con cambios de alcance para Comisiones    	                #			
#########################################################################################################

##############
# VARIABLES #
##############
ENTIDAD=EXTRCTRTRMNLS0070
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_FECHA_EJEC=$1

VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_SFTP_PUERTO_OUT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_PUERTO_OUT';"`
VAL_SFTP_USER_OUT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_USER_OUT';"`
VAL_SFTP_HOSTNAME_OUT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_HOSTNAME_OUT';"`
VAL_SFTP_PASS_OUT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_PASS_OUT';"`
VAL_SFTP_RUTA_OUT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_RUTA_OUT';"`
VAL_NOM_ARCHIVO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_EXECUTOR_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_CORES';"`
VAL_DIR_HDFS_CAT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_DIR_HDFS_CAT';"` 
VAL_SFTP_NOM_ARCHIVO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_SFTP_NOM_ARCHIVO';"`
VAL_SFTP_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_SFTP_RUTA';"` 
VTFINAL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VTFINAL';"` 

#PARAMETROS GENERICOS
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_SFTP_HOSTNAME=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND PARAMETRO = 'VAL_SFTP_HOST';"` 
VAL_SFTP_PORT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND PARAMETRO = 'VAL_SFTP_PORT';"` 
VAL_SFTP_USER=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND PARAMETRO = 'VAL_SFTP_USER_ADMVENTAS';"` 
VAL_SFTP_PASS=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND PARAMETRO = 'VAL_SFTP_PASS_ADMVENTAS';"` 

VAL_RUTA_ARCHIVO=$VAL_RUTA/input
VAL_TRREMOTEDIR=`echo $VAL_SFTP_RUTA|sed "s/\~}</ /g"`
VAL_REMOTEDIRFINAL=${VAL_TRREMOTEDIR}

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FECHA_PARAM=`date -d "${VAL_FECHA_EJEC}"  +"%Y%m04"`
VAL_FEC_AYER=`date -d "${VAL_FECHA_PARAM} -1 day"  +"%Y%m%d"`
VAL_DIA_UNO=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m01"` #fecha fin
VAL_FECHA_INI=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m01"` #fecha ini
VAL_FECHA_FORMATO_PRE=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m%d"`
VAL_FECHA_FORMATO=`date -d "${VAL_DIA_UNO} -1 day"  +"%d/%m/%Y"`
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/logs/OTC_T_EXT_TERMINALES_$VAL_DIA$VAL_HORA.log  ## ojo LOG
VAL_NOM_ARCHIVO_PREVIO=EXT_TERMINALES.txt

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_FECHA_EJEC" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$ETAPA" ] || 
    [ -z "$VAL_SFTP_PUERTO_OUT" ] || 
    [ -z "$VAL_SFTP_USER_OUT" ] || 
    [ -z "$VAL_SFTP_HOSTNAME_OUT" ] || 
    [ -z "$VAL_SFTP_PASS_OUT" ] || 
    [ -z "$VAL_SFTP_RUTA_OUT" ] || 
    [ -z "$VAL_NOM_ARCHIVO" ] || 
	[ -z "$VAL_SFTP_HOSTNAME" ] || 
	[ -z "$VAL_SFTP_PORT" ] || 
	[ -z "$VAL_SFTP_USER" ] || 
	[ -z "$VAL_SFTP_PASS" ] || 
	[ -z "$VAL_SFTP_RUTA" ] || 
	[ -z "$VAL_DIR_HDFS_CAT" ] ||
	[ -z "$VTFINAL" ] ||
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion AJUSTES_TERMINALES.xlsx===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
#PASO 1: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS CSV DESDE EL SERVIDOR SFTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "1" ]; then
echo "==== Realiza la transferencia de AJUSTES_TERMINALES.xlsx desde el servidor SFTP a BigData ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Servidor: $VAL_SFTP_HOSTNAME" 2>&1 &>> $VAL_LOG
echo "Puerto: $VAL_SFTP_PORT" 2>&1 &>> $VAL_LOG
echo "Ruta: $VAL_SFTP_RUTA" 2>&1 &>> $VAL_LOG

rm -r ${VAL_RUTA}/input/*

#ftp -inv $VAL_SFTP_HOSTNAME $VAL_SFTP_PUERTO <<EOF 2>&1 &>> $VAL_LOG
#user $VAL_SFTP_USER $VAL_SFTP_PASS
#bin
#pwd
#cd ${VAL_REMOTEDIRFINAL}
#lcd ${VAL_RUTA_ARCHIVO}
#mget ${VAL_SFTP_NOM_ARCHIVO}
#bye
#EOF

sshpass -p $VAL_SFTP_PASS sftp -P $VAL_SFTP_PORT -oBatchMode=no $VAL_SFTP_USER@$VAL_SFTP_HOSTNAME  << EOF | tee -a 2>&1 &>> $VAL_LOG;
cd $VAL_SFTP_RUTA
pwd
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_SFTP_NOM_ARCHIVO}
bye
EOF

chmod 777 $VAL_RUTA/input/*

VAL_ERRORES=`egrep -i 'ERROR - En la transferencia del archivo|not found|No such file or directory|Permission denied' $VAL_LOG | wc -l`
VAL_UPLOAD=`egrep 'Uploading|Fetching' $VAL_LOG | wc -l`
if [ $VAL_ERRORES -ne 0 ]; then
	echo "==== ERROR - En la transferencia del archivo ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	exit 1
else
	if [ $VAL_UPLOAD -ne 0 ]; then
		echo "==== OK - La transferencia de los archivos desde el servidor SFTP se realiza con EXITO ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	else
		echo "==== ERROR: - En la transferencia de los archivos desde el servidor SFTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
		exit 1
	fi
fi

echo "==== Finaliza la transferencia de AJUSTES_TERMINALES.xlsx===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG

ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#HACE EL LLAMADO AL PYTHON QUE REALIZA LA CONVERSION DEL ARCHIVO  XLSX a tabla en Hive
if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo "==== Hace el llamado al python que realiza la conversion del archivo xlsx a tabla en Hive ====" 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue capa_semantica \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/otc_t_ajustes_terminales.py \
--rutain=$VAL_RUTA/input/$VAL_SFTP_NOM_ARCHIVO \
--tablaout=$VAL_DIR_HDFS_CAT \
--tipo=overwrite 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL PYTHON
echo "==== Valida ejecucion del python que hace la conversion de excel a Hive ====" 2>&1 &>> $VAL_LOG
error_py=`egrep 'AnalysisException|TypeError:|FAILED:|Error|Table not found|Table already exists|Vertex|No such file or directory' $VAL_LOG | wc -l`
if [ $error_py -eq 0 ];then
	echo "==== OK - La ejecucion del python  es EXITOSO ====" 2>&1 &>> $VAL_LOG
else
	echo "==== ERROR - En la ejecucion del python  ====" 2>&1 &>> $VAL_LOG
	exit 1
fi		

ETAPA=3
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#SE REALIZA LOS CRUCES PARA GENERAR LA INFORMACION EN LA TABLA FINAL OTC_T_TERMINALES_SIMCARDS
if [ "$ETAPA" = "3" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 3: Ejecuta subproceso PySpark otc_t_ext_terminales.py ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Fecha inicio:            $VAL_FECHA_INI" 2>&1 &>> $VAL_LOG
echo "Fecha Fin:               $VAL_DIA_UNO" 2>&1 &>> $VAL_LOG
echo "Fecha antes de ayer:     $VAL_FECHA_FORMATO_PRE" 2>&1 &>> $VAL_LOG
echo "Ultimo dia:              $VAL_FECHA_FORMATO" 2>&1 &>> $VAL_LOG
echo "Tabla Destino:           db_reportes.otc_t_ext_terminales_ajst" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.kryo.registrator=com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator \
--conf spark.sql.extensions=com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.datasource.hive.warehouse.read.mode=DIRECT_READER_V2 \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--conf spark.hadoop.hive.metastore.uris="thrift://quisrvbigdata1.otecel.com.ec:9083,thrift://quisrvbigdata10.otecel.com.ec:9083" \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--conf spark.port.maxRetries=100 \
--master $VAL_MASTER \
--name $ENTIDAD \
--queue capa_semantica \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/otc_t_ext_terminales.py \
--ventidad=$ENTIDAD \
--vfecha_fin=$VAL_DIA_UNO \
--vfecha_inicio=$VAL_FECHA_INI \
--vtfinal=$VTFINAL \
--vfecha_antes_ayer=$VAL_FECHA_FORMATO_PRE \
--vultimo_dia_act_frmt=$VAL_FECHA_FORMATO 2>&1 &>> $VAL_LOG

error_spark=`egrep 'Traceback|error: argument|invalid syntax|An error occurred|Caused by:|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|ImportError|SyntaxError' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark otc_t_ext_terminales.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark otc_t_ext_terminales.py ====" 2>&1 &>> $VAL_LOG
exit 1
fi

ETAPA=4
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 3 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='4' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#LEE TABLA otc_t_ext_terminales_ajst Y GENERA ARCHIVO TXT EN RUTA OUTPUT
if [ "$ETAPA" = "4" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 4: Lee tabla otc_t_ext_terminales_ajst y genera archivo txt en ruta output ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Fecha formato:            $VAL_FECHA_FORMATO" 2>&1 &>> $VAL_LOG
echo "Fecha Inicio:             $VAL_FECHA_INI" 2>&1 &>> $VAL_LOG
echo "Dia uno:                  $VAL_DIA_UNO" 2>&1 &>> $VAL_LOG
echo "Archivo Destino:          ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO_PREVIO" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/genera_archivo.py \
--ventidad=$ENTIDAD \
--vval_fecha_formato=$VAL_FECHA_FORMATO \
--vval_dia_uno=$VAL_DIA_UNO \
--vfecha_inicio=$VAL_FECHA_INI \
--vArchivo=${VAL_RUTA}/output/$VAL_NOM_ARCHIVO 2>&1 &>> $VAL_LOG

error_spark=`egrep 'Traceback|error: argument|invalid syntax|An error occurred|Caused by:|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|ImportError|SyntaxError' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark genera_archivo.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark genera_archivo.py ====" 2>&1 &>> $VAL_LOG
exit 1
fi

#CONVIERTE LOS NOMBRES DE LOS CAMPOS DE MINUSCULAS A MAYUSCULAS
sed -i -e '1 s/\(.*\)/\U\1/' ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO
#CAMBIA EL ENCODING DEL ARCHIVO PARA QUE NO GENERE CARACTERES ESPECIALES
#iconv -f utf8 -t ascii//TRANSLIT ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO_PREVIO > ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO

#VERIFICA SI EL ARCHIVO TXT CONTIENE DATOS
echo "==== Valida si el archivo TXT contiene datos ====" 2>&1 &>> $VAL_LOG
cant_reg=`wc -l ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO` 
echo $cant_reg 2>&1 &>> $VAL_LOG
cant_reg=`echo ${cant_reg}|cut -f1 -d" "` 
cant_reg=`expr $cant_reg + 0` 
	if [ $cant_reg -ne 0 ]; then
			echo "==== OK - El archivo TXT contiene datos para transferir al servidor SFTP ====" 2>&1 &>> $VAL_LOG
		else
			echo "==== ERROR - El archivo TXT no contiene datos para transferir al servidor SFTP ====" 2>&1 &>> $VAL_LOG
			exit 1
	fi
ETAPA=5
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 4 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='5' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

vFTP_NOM_ARCHIVO_FORMATO='Extractor_Terminales_Julio.txt'
#CREA FUNCION PARA LA EXPORTACION DEL ARCHIVO A RUTA SFTP Y REALIZA LA TRANSFERENCIA
if [ "$ETAPA" = "5" ]; then
echo "==== Crea funcion para la exportacion del archivo a ruta SFTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
function exportar()
{
    /usr/bin/expect << EOF 2>&1 &>> $VAL_LOG
		set timeout -1
		spawn sftp ${VAL_SFTP_USER_OUT}@${VAL_SFTP_HOSTNAME_OUT} ${VAL_SFTP_PUERTO_OUT}
		expect "password:"
		send "${VAL_SFTP_PASS_OUT}\n"
		expect "sftp>"
		send "cd ${VAL_SFTP_RUTA_OUT}\n"
		expect "sftp>"
		send "put ${VAL_RUTA}/output/${VAL_NOM_ARCHIVO} $(basename ${vFTP_NOM_ARCHIVO_FORMATO})\n"
		expect "sftp>"
		send "exit\n"
		interact
EOF
}
# send "put ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO\n"
# send "put ${VAL_RUTA}/output/${VAL_NOM_ARCHIVO} $(basename ${vFTP_NOM_ARCHIVO_FORMATO})\n"

#REALIZA LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA SFTP
echo  "==== Inicia exportacion del archivo txt al servidor SFTP ====" 2>&1 &>> $VAL_LOG
echo "Host SFTP: $VAL_SFTP_HOSTNAME_OUT" 2>&1 &>> $VAL_LOG
echo "Puerto SFTP: $VAL_SFTP_PUERTO_OUT" 2>&1 &>> $VAL_LOG
echo "Usuario SFTP: $VAL_SFTP_USER_OUT" 2>&1 &>> $VAL_LOG
echo "Password SFTP: $VAL_SFTP_PASS_OUT" 2>&1 &>> $VAL_LOG
echo "Ruta SFTP: $VAL_SFTP_RUTA_OUT" 2>&1 &>> $VAL_LOG
exportar $VAL_NOM_ARCHIVO 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DE LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA SFTP
echo "==== Valida transferencia del archivo TXT al servidor SFTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
VAL_ERROR_FTP=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
	if [ $VAL_ERROR_FTP -ne 0 ]; then
		echo "==== ERROR - En la transferencia del archivo TXT al servidor SFTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
		exit 1
		else
		echo "==== OK - La transferencia del archivo TXT al servidor SFTP es EXITOSA ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	fi
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 5 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
	
echo "==== Finaliza ejecucion del proceso Extractor de Terminales ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
