set -e
#########################################################################################################
# NOMBRE: SH_TERMINALES_SIMCARDS_ACTUAL.sh   	     	      								            #
# DESCRIPCION:																							#
#   Shell que realiza el proceso del mes actual para generar la informacio de terminales simcards		#
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
ENTIDAD=EXTRCTRTRMNLS0080
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS DEFINIDOS EN LA TABLA params_des
VAL_FECHA_EJEC=$1
#VAL_RUTA=$2
VAL_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
sh -x $VAL_RUTA/bin/OTC_T_AJUSTES_TERMINALES.sh

ETAPA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_FTP_PUERTO1=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO1';"`
VAL_FTP_PUERTO2=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO2';"`
VAL_FTP_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_USER';"`
VAL_FTP_HOSTNAME=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_HOSTNAME';"`
VAL_FTP_PASS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PASS';"`
VAL_FTP_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_RUTA';"`
HIVEDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_BASE_DATOS';"`
VAL_TABLA_TC=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_TC';"`
VAL_TABLA_RDR=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_RDR';"`
VAL_TABLA_T=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_T';"`
VAL_TABLA_UO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_UO';"`
VAL_TABLA_CANAL=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_CANAL';"`
VAL_TABLA_SEG=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_SEG';"`
VAL_TABLA_CST=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_CST';"`
VAL_USUARIO4=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO4';"`
VAL_USUARIO_FINAL=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO_FINAL';"`
VAL_MESES=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MESES';"`
VAL_MESES1=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MESES1';"`
VAL_MESES2=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MESES2';"`
VAL_FTP_PUERTO_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO_OUT';"`
VAL_FTP_USER_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_USER_OUT';"`
VAL_FTP_HOSTNAME_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_HOSTNAME_OUT';"`
VAL_FTP_PASS_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PASS_OUT';"`
VAL_FTP_RUTA_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_RUTA_OUT';"`
VAL_NOM_ARCHIVO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_MASTER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_EXECUTOR_CORES=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_CORES';"`

#PARAMETROS GENERICOS
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FEC_AYER=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y%m%d"`
VAL_DIA_UNO=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m01"` #fecha fin
VAL_FECHA_INI=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m01"` #fecha ini
VAL_FECHA_FORMATO_PRE=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m%d"`
VAL_FECHA_FORMATO=`date -d "${VAL_DIA_UNO} -1 day"  +"%d/%m/%Y"`
VAL_DIA_UNO_MES_SIG_FRMT=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y-%m-01"`
VAL_MES=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m"`
VAL_SOLO_ANIO=`echo $VAL_FECHA_INI | cut -c1-4`
VAL_SOLO_MES=`echo $VAL_FECHA_INI | cut -c5-6`
VAL_MESES_ATRAS=$(date -d "$(date -d $VAL_DIA_UNO +%Y%m%d) -${VAL_MESES} month" +%Y%m%d)
VAL_MESES_ATRAS1=$(date -d "$(date -d $VAL_DIA_UNO +%Y%m%d) -${VAL_MESES1} month" +%Y%m%d)
VAL_MESES_ATRAS2=$(date -d "$(date -d $VAL_DIA_UNO +%Y%m%d) -${VAL_MESES2} month" +%Y%m%d)
VAL_FECHA_FORM_INI=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y-%m-01"`
VAL_FECHA_FORMATO_INI=$VAL_FECHA_FORM_INI" 00:00:00"
VAL_D1_MES_ANT=`date -d "${VAL_DIA_UNO} -1 month"  +"%Y-%m-%d"`
VAL_TS_INI=$VAL_D1_MES_ANT" 00:00:00"
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/logs/SH_TERMINALES_SIMCARDS_$VAL_DIA$VAL_HORA.log
VAL_RUTA_ARCHIVO=$VAL_RUTA/input
VAL_RUTA_ARCHIVO_MP=$VAL_RUTA/input/$VAL_NOM_ARCHIVO_MP
VAL_NOM_ARCHIVO_PREVIO=EXT_TERMINALES.txt

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_FECHA_EJEC" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$ETAPA" ] || 
    [ -z "$VAL_FTP_PUERTO1" ] || 
    [ -z "$VAL_FTP_PUERTO2" ] || 
    [ -z "$VAL_FTP_USER" ] || 
    [ -z "$VAL_FTP_HOSTNAME" ] || 
    [ -z "$VAL_FTP_PASS" ] || 
    [ -z "$VAL_FTP_RUTA" ] || 
    [ -z "$HIVEDB" ] || 
    [ -z "$VAL_TABLA_TC" ] || 
    [ -z "$VAL_TABLA_RDR" ] || 
    [ -z "$VAL_TABLA_T" ] || 
    [ -z "$VAL_TABLA_UO" ] || 
    [ -z "$VAL_TABLA_CANAL" ] || 
    [ -z "$VAL_TABLA_SEG" ] || 
    [ -z "$VAL_TABLA_CST" ] || 
    [ -z "$VAL_USUARIO4" ] || 
    [ -z "$VAL_USUARIO_FINAL" ] || 
    [ -z "$VAL_MESES" ] || 
    [ -z "$VAL_MESES1" ] || 
    [ -z "$VAL_MESES2" ] || 
    [ -z "$VAL_FTP_PUERTO_OUT" ] || 
    [ -z "$VAL_FTP_USER_OUT" ] || 
    [ -z "$VAL_FTP_HOSTNAME_OUT" ] || 
    [ -z "$VAL_FTP_PASS_OUT" ] || 
    [ -z "$VAL_FTP_RUTA_OUT" ] || 
    [ -z "$VAL_NOM_ARCHIVO" ] || 
    [ -z "$VAL_TIPO_CARGA" ] || 
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#HACE EL LLAMADO AL HQL QUE REALIZA LOS CRUCES PARA GENERAR LA INFORMACION EN LA TABLA FINAL OTC_T_TERMINALES_SIMCARDS
if [ "$ETAPA" = "1" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 1: Ejecuta subproceso PySpark otc_t_ext_terminales.py ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Fecha inicio:            $VAL_FECHA_INI" 2>&1 &>> $VAL_LOG
echo "Fecha Fin:               $VAL_DIA_UNO" 2>&1 &>> $VAL_LOG
echo "Fecha antes de ayer:     $VAL_FECHA_FORMATO_PRE" 2>&1 &>> $VAL_LOG
echo "Anio mes:                $VAL_MES" 2>&1 &>> $VAL_LOG
echo "Dia mes siguiente:       $VAL_DIA_UNO_MES_SIG_FRMT" 2>&1 &>> $VAL_LOG
echo "Primer dia:              $VAL_FECHA_FORMATO_INI" 2>&1 &>> $VAL_LOG
echo "Ultimo dia:              $VAL_FECHA_FORMATO" 2>&1 &>> $VAL_LOG
echo "Meses atras:             $VAL_MESES_ATRAS - $VAL_MESES_ATRAS1 - $VAL_MESES_ATRAS2" 2>&1 &>> $VAL_LOG
echo "Primer dia mes anterior: $VAL_TS_INI" 2>&1 &>> $VAL_LOG
echo "Usuario:                 $VAL_USUARIO4" 2>&1 &>> $VAL_LOG
echo "Usuario Final:           $VAL_USUARIO_FINAL" 2>&1 &>> $VAL_LOG
echo "Tabla Destino:           $vTablaDestino" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-*.jar \
--conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
--conf spark.security.credentials.hiveserver2.enabled=false \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.datasource.hive.warehouse.read.via.llap=false \
--conf spark.datasource.hive.warehouse.load.staging.dir=/tmp \
--conf spark.datasource.hive.warehouse.read.jdbc.mode=cluster \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/otc_t_ext_terminales.py \
--ventidad=$ENTIDAD \
--vhivebd=$HIVEDB \
--vfecha_fin=$VAL_DIA_UNO \
--vfecha_inicio=$VAL_FECHA_INI \
--vfecha_antes_ayer=$VAL_FECHA_FORMATO_PRE \
--vdia_uno_mes_sig_frmt=$VAL_DIA_UNO_MES_SIG_FRMT \
--vultimo_dia_act_frmt=$VAL_FECHA_FORMATO \
--vanio_mes=$VAL_MES \
--vsolo_anio=$VAL_SOLO_ANIO \
--vsolo_mes=$VAL_SOLO_MES \
--vfecha_meses_atras=$VAL_MESES_ATRAS \
--vfecha_meses_atras1=$VAL_MESES_ATRAS1 \
--vfecha_meses_atras2=$VAL_MESES_ATRAS2 \
--vdia_uno_mes_act_frmt="${VAL_FECHA_FORMATO_INI}" \
--vdia_uno_mes_ant_frmt="${VAL_TS_INI}" \
--vval_usuario4=${VAL_USUARIO4} \
--vTablaDestino=$vTablaDestino \
--vval_usuario_final=${VAL_USUARIO_FINAL} 2>&1 &>> $VAL_LOG

error_spark=`egrep 'Traceback|error: argument|invalid syntax|An error occurred|Caused by:|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|ImportError|SyntaxError' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark otc_t_ext_terminales.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark otc_t_ext_terminales.py ====" 2>&1 &>> $VAL_LOG
exit 1
fi

ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params_des set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#LEE TABLA TERMINALES SIMCARDS Y GENERA ARCHIVO TXT EN RUTA OUTPUT
if [ "$ETAPA" = "2" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 2: Lee tabla otc_t_ext_terminales_ajst y genera archivo txt en ruta output ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Fecha formato:            $VAL_FECHA_FORMATO" 2>&1 &>> $VAL_LOG
echo "Fecha Inicio:             $VAL_FECHA_INI" 2>&1 &>> $VAL_LOG
echo "Dia uno:                  $VAL_DIA_UNO" 2>&1 &>> $VAL_LOG
echo "Archivo Destino:          ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO_PREVIO" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-*.jar \
--conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
--conf spark.security.credentials.hiveserver2.enabled=false \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.datasource.hive.warehouse.read.via.llap=false \
--conf spark.datasource.hive.warehouse.load.staging.dir=/tmp \
--conf spark.datasource.hive.warehouse.read.jdbc.mode=cluster \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--conf spark.port.maxRetries=100 \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
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
			echo "==== OK - El archivo TXT contiene datos para transferir al servidor FTP ====" 2>&1 &>> $VAL_LOG
		else
			echo "==== ERROR - El archivo TXT no contiene datos para transferir al servidor FTP ====" 2>&1 &>> $VAL_LOG
			exit 1
	fi
ETAPA=3
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params_des set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

vFTP_NOM_ARCHIVO_FORMATO='Extractor_Terminales_TEST.txt'
#CREA FUNCION PARA LA EXPORTACION DEL ARCHIVO A RUTA FTP Y REALIZA LA TRANSFERENCIA
if [ "$ETAPA" = "3" ]; then
echo "==== Crea funcion para la exportacion del archivo a ruta FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
function exportar()
{
    /usr/bin/expect << EOF 2>&1 &>> $VAL_LOG
		set timeout -1
		spawn sftp ${VAL_FTP_USER_OUT}@${VAL_FTP_HOSTNAME_OUT} ${VAL_FTP_PUERTO_OUT}
		expect "password:"
		send "${VAL_FTP_PASS_OUT}\n"
		expect "sftp>"
		send "cd ${VAL_FTP_RUTA_OUT}\n"
		expect "sftp>"
		send "put ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO\n"
		expect "sftp>"
		send "exit\n"
		interact
EOF
}

# send "put ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO\n"
# send "put ${VAL_RUTA}/output/${VAL_NOM_ARCHIVO} $(basename ${vFTP_NOM_ARCHIVO_FORMATO})\n"

#REALIZA LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo  "==== Inicia exportacion del archivo txt al servidor SFTP ====" 2>&1 &>> $VAL_LOG
echo "Host SFTP: $VAL_FTP_HOSTNAME_OUT" 2>&1 &>> $VAL_LOG
echo "Puerto SFTP: $VAL_FTP_PUERTO_OUT" 2>&1 &>> $VAL_LOG
echo "Usuario SFTP: $VAL_FTP_USER_OUT" 2>&1 &>> $VAL_LOG
echo "Password SFTP: $VAL_FTP_PASS_OUT" 2>&1 &>> $VAL_LOG
echo "Ruta SFTP: $VAL_FTP_RUTA_OUT" 2>&1 &>> $VAL_LOG
exportar $VAL_NOM_ARCHIVO 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DE LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo "==== Valida transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
VAL_ERROR_FTP=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
	if [ $VAL_ERROR_FTP -ne 0 ]; then
		echo "==== ERROR - En la transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
		exit 1
		else
		echo "==== OK - La transferencia del archivo TXT al servidor FTP es EXITOSA ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	fi
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 3 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params_des set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
	
echo "==== Finaliza ejecucion del proceso Extractor de Terminales ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG

