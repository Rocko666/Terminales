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
# 2023-07-27	Cristian Ortiz	BIGD-62 Terminales, separacion extractor,ajustes,                       #								
#########################################################################################################
##############
# VARIABLES #
##############
ENTIDAD=SHTRMNLSMCRDS0010
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_FECHA_EJEC=$1
VAL_RUTA=$2
#VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`

ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_NOM_ARCHIVO1_0=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_0';"`
VAL_NOM_ARCHIVO1_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_1';"`
VAL_NOM_ARCHIVO1_2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_2';"`
VAL_NOM_ARCHIVO1_3=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_3';"`
VAL_NOM_ARCHIVO1_4=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_4';"`
VAL_NOM_ARCHIVO_MP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO_MP';"`
VAL_NOM_ARCHIVO2_0=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO2_0';"`
VAL_NOM_ARCHIVO2_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO2_1';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_BASE_DATOS';"`
VAL_TABLA_TC=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_TC';"`
VAL_TABLA_RDR=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_RDR';"`
VAL_TABLA_T=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_T';"`
VAL_TABLA_UO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_UO';"`
VAL_TABLA_CANAL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_CANAL';"`
VAL_TABLA_SEG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_SEG';"`
VAL_TABLA_CST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_CST';"`
VAL_USUARIO4=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO4';"`
VAL_USUARIO_FINAL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO_FINAL';"`
VAL_MESES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MESES';"`
VAL_MESES1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MESES1';"`
VAL_MESES2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MESES2';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_EXECUTOR_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_CORES';"`
VAL_SFTP_RUTA_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_RUTA_1';"`
VAL_SFTP_RUTA_2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_RUTA_2';"`
vTablaDestino=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VTABLADESTINO';"`
VAL_QUEUE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_QUEUE';"`

#PARAMETROS GENERICOS
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_SFTP_USER=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_USER_MERCADEO';"`
VAL_SFTP_PORT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PORT';"`
VAL_SFTP_HOSTNAME=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_HOST';"`
VAL_SFTP_PASS=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PASS_MERCADEO';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FEC_AYER=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y%m%d"`
VAL_DIA_UNO=$VAL_FEC_AYER
VAL_F_MESES_AUX=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y%m01"`
VAL_FECHA_INI=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m01"` #fecha ini
VAL_FECHA_AUX2=`date -d "${VAL_FECHA_INI} +1 month"  +"%Y%m%d"` #fecha ini
VAL_FECHA_FORMATO_PRE=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m%d"`
VAL_FECHA_FORMATO=`date -d "${VAL_DIA_UNO} -1 day"  +"%d/%m/%Y"`
VAL_DIA_UNO_MES_SIG_FRMT=`date -d "${VAL_FEC_AYER}"  +"%Y-%m-01"`
VAL_MES=`date -d "${VAL_DIA_UNO} -1 day"  +"%Y%m"`
VAL_SOLO_ANIO=`echo $VAL_FECHA_INI | cut -c1-4`
VAL_SOLO_MES=`echo $VAL_FECHA_INI | cut -c5-6`
VAL_MESES_ATRAS=$(date -d "$(date -d $VAL_FECHA_AUX2 +%Y%m01) -${VAL_MESES} month" +%Y%m%d)
VAL_MESES_ATRAS1=$(date -d "$(date -d $VAL_FECHA_AUX2 +%Y%m01) -${VAL_MESES1} month" +%Y%m%d)
VAL_MESES_ATRAS2=$(date -d "$(date -d $VAL_FECHA_AUX2 +%Y%m01) -${VAL_MESES2} month" +%Y%m%d)
VAL_FECHA_FORM_INI=`date -d "${VAL_FEC_AYER}"  +"%Y-%m-01"`
VAL_FECHA_FORMATO_INI=$VAL_FECHA_FORM_INI" 00:00:00"
VAL_D1_MES_ANT=`date -d "${VAL_F_MESES_AUX} -1 month"  +"%Y-%m-%d"`
VAL_TS_INI=$VAL_D1_MES_ANT" 00:00:00"
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/logs/SH_TERMINALES_SIMCARDS_$VAL_DIA$VAL_HORA.log  ## ojo ---> LOGS
VAL_RUTA_ARCHIVO=$VAL_RUTA/input
VAL_NOM_ARCHIVO1_2_SIN=`echo $VAL_NOM_ARCHIVO1_2|sed "s/\*/ /g"`
VAL_NOM_ARCHIVO1_2_CON_PRE=`echo $VAL_NOM_ARCHIVO1_2|sed "s/\"//g"`
VAL_NOM_ARCHIVO1_2_CON=`echo $VAL_NOM_ARCHIVO1_2_CON_PRE|sed "s/\*/_/g"`
VAL_NOM_ARCHIVO1_3_SIN=`echo $VAL_NOM_ARCHIVO1_3|sed "s/\*/ /g"`
VAL_NOM_ARCHIVO1_3_CON_PRE=`echo $VAL_NOM_ARCHIVO1_3|sed "s/\"//g"`
VAL_NOM_ARCHIVO1_3_CON=`echo $VAL_NOM_ARCHIVO1_3_CON_PRE|sed "s/\*/_/g"`
VAL_RUTA_ARCHIVO_1_2=$VAL_RUTA/input/$VAL_NOM_ARCHIVO1_2_CON
VAL_RUTA_ARCHIVO_1_0=$VAL_RUTA/input/$VAL_NOM_ARCHIVO1_0
VAL_RUTA_ARCHIVO_2_1=$VAL_RUTA/input/$VAL_NOM_ARCHIVO2_1
VAL_RUTA_ARCHIVO_2_0=$VAL_RUTA/input/$VAL_NOM_ARCHIVO2_0
VAL_RUTA_ARCHIVO_1_3=$VAL_RUTA/input/$VAL_NOM_ARCHIVO1_3_CON
VAL_RUTA_ARCHIVO_1_4=$VAL_RUTA/input/$VAL_NOM_ARCHIVO1_4
VAL_RUTA_ARCHIVO_MP=$VAL_RUTA/input/$VAL_NOM_ARCHIVO_MP

VAL_TRREMOTEDIR_1=`echo $VAL_SFTP_RUTA_1|sed "s/\~}</ /g"`  ## espacios en blanco reemplz
VAL_TRREMOTEDIR1=$(echo "$VAL_TRREMOTEDIR_1" | sed "s/\"//g") ## para comilla doble

VAL_TRREMOTEDIR_2=`echo $VAL_SFTP_RUTA_2|sed "s/\~}</ /g"`  ## espacios en blanco reemplz
VAL_TRREMOTEDIR2=$(echo "$VAL_TRREMOTEDIR_2" | sed "s/\"//g") ## para comilla doble

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_FECHA_EJEC" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$ETAPA" ] || 
    [ -z "$VAL_SFTP_USER" ] || 
    [ -z "$VAL_SFTP_PORT" ] || 
    [ -z "$VAL_SFTP_HOSTNAME" ] || 
    [ -z "$VAL_SFTP_PASS" ] || 
    [ -z "$VAL_SFTP_RUTA_1" ] || 
    [ -z "$VAL_SFTP_RUTA_2" ] || 
    [ -z "$VAL_NOM_ARCHIVO1_0" ] || 
    [ -z "$VAL_NOM_ARCHIVO1_2" ] || 
    [ -z "$VAL_NOM_ARCHIVO1_3" ] || 
    [ -z "$VAL_NOM_ARCHIVO1_4" ] || 
    [ -z "$VAL_NOM_ARCHIVO_MP" ] || 
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
    [ -z "$VAL_TIPO_CARGA" ] || 
    [ -z "$vTablaDestino" ] || 
    [ -z "$VAL_QUEUE" ] || 
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "=======================================================================================================" > $VAL_LOG
echo "==== Inicia ejecucion del proceso BI CS Terminales Simcards  ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" 2>&1 &>> $VAL_LOG
echo "Fecha Ejecucion: $VAL_FECHA_EJEC" 2>&1 &>> $VAL_LOG
echo "Fecha Inicio: $VAL_FECHA_INI" 2>&1 &>> $VAL_LOG
echo "Fecha Fin: $VAL_DIA_UNO" 2>&1 &>> $VAL_LOG
echo "Fecha Ayer: $VAL_FEC_AYER" 2>&1 &>> $VAL_LOG
echo "Fecha F.Pre: $VAL_FECHA_FORMATO_PRE" 2>&1 &>> $VAL_LOG
echo "Fecha Formato: $VAL_FECHA_FORMATO" 2>&1 &>> $VAL_LOG
echo "Fecha D1 Mes sig: $VAL_DIA_UNO_MES_SIG_FRMT" 2>&1 &>> $VAL_LOG
echo "Fecha Mes: $VAL_MES" 2>&1 &>> $VAL_LOG
echo "Fecha Solo anio: $VAL_SOLO_ANIO" 2>&1 &>> $VAL_LOG
echo "Fecha solo mes: $VAL_SOLO_MES" 2>&1 &>> $VAL_LOG
echo "Fecha M Atras: $VAL_MESES_ATRAS" 2>&1 &>> $VAL_LOG
echo "Fecha M Atras1: $VAL_MESES_ATRAS1" 2>&1 &>> $VAL_LOG
echo "Fecha M Atras2: $VAL_MESES_ATRAS2" 2>&1 &>> $VAL_LOG
echo "Fecha F ini: $VAL_FECHA_FORM_INI" 2>&1 &>> $VAL_LOG
echo "Fecha D1 mes ant: $VAL_D1_MES_ANT" 2>&1 &>> $VAL_LOG

#PASO 1: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "1" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 1: Realiza la transferencia de los archivos en formato Excel desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Servidor: $VAL_SFTP_HOSTNAME" 2>&1 &>> $VAL_LOG
echo "Ruta: $VAL_TRREMOTEDIR1" 2>&1 &>> $VAL_LOG
echo "Puerto: $VAL_SFTP_PORT" 2>&1 &>> $VAL_LOG
echo "Archivo 1: $VAL_NOM_ARCHIVO1_0" 2>&1 &>> $VAL_LOG
echo "Archivo 2: $VAL_NOM_ARCHIVO1_2_SIN" 2>&1 &>> $VAL_LOG
echo "Archivo 3: $VAL_NOM_ARCHIVO1_3_SIN" 2>&1 &>> $VAL_LOG
echo "Archivo 4: $VAL_NOM_ARCHIVO1_4" 2>&1 &>> $VAL_LOG
#ELIMINA LOS ARCHIVOS EXCEL DE RUTA INPUT
rm -r ${VAL_RUTA}/input/*

#ftp -inv $VAL_FTP_HOSTNAME $VAL_FTP_PUERTO1 <<EOF 2>&1 &>> $VAL_LOG
#user $VAL_FTP_USER $VAL_FTP_PASS
#bin
#pwd
#cd ${VAL_FTP_RUTA}
#lcd ${VAL_RUTA_ARCHIVO}
#mget ${VAL_NOM_ARCHIVO1_0}
#mget ${VAL_NOM_ARCHIVO1_2_SIN}
#mget ${VAL_NOM_ARCHIVO1_3_SIN}
#mget ${VAL_NOM_ARCHIVO1_4}
#bye
#EOF

sshpass -p $VAL_SFTP_PASS sftp -P $VAL_SFTP_PORT -oBatchMode=no $VAL_SFTP_USER@$VAL_SFTP_HOSTNAME  << EOF | tee -a 2>&1 &>> $VAL_LOG;
cd $VAL_TRREMOTEDIR1
pwd
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_NOM_ARCHIVO1_0}
mget ${VAL_NOM_ARCHIVO1_2_SIN}
mget ${VAL_NOM_ARCHIVO1_3_SIN}
mget ${VAL_NOM_ARCHIVO1_4}
mget ${VAL_NOM_ARCHIVO1_1}
mget ${VAL_NOM_ARCHIVO_MP}
bye
EOF

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

#REALIZA COPIA DE LOS ARCHIVOS 100 Nueva Categoria Y ASIGNACION CANAL DE VENTAS v3 PARA ELIMINAR ESPACIOS EN EL NOMBRE DE ACUERDO A LAS VARIABLES CONFIGURADAS EN params
echo "==== Realiza copia de los archivos 100 Nueva Categoria y Asignacion canal de ventas para eliminar espacios en el nombre ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
cd $VAL_RUTA/input
ls -altrh *.xlsx
ix=0
for file in *.xlsx; do
    new_file="${file// /_}"
    if [ "$file" != "$new_file" ]; then
        mv "$file" "$new_file"
        ix=$(($ix + 1))
    fi
done

ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 2: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "2" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 2: Realiza la transferencia de los archivos en formato Excel desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Servidor: $VAL_SFTP_HOSTNAME" 2>&1 &>> $VAL_LOG
echo "Ruta: $VAL_TRREMOTEDIR2" 2>&1 &>> $VAL_LOG
echo "Puerto: $VAL_SFTP_PORT" 2>&1 &>> $VAL_LOG
echo "Archivo 1: $VAL_NOM_ARCHIVO2_0" 2>&1 &>> $VAL_LOG
echo "Archivo 2: $VAL_NOM_ARCHIVO2_1" 2>&1 &>> $VAL_LOG
#ftp -inv $VAL_FTP_HOSTNAME $VAL_FTP_PUERTO2 <<EOF 2>&1 &>> $VAL_LOG
#user $VAL_FTP_USER $VAL_FTP_PASS
#bin
#cd ${VAL_FTP_RUTA}
#lcd ${VAL_RUTA_ARCHIVO}
#mget ${VAL_NOM_ARCHIVO2_0}
#mget ${VAL_NOM_ARCHIVO2_1}
#bye
#EOF

sshpass -p $VAL_SFTP_PASS sftp -P $VAL_SFTP_PORT -oBatchMode=no $VAL_SFTP_USER@$VAL_SFTP_HOSTNAME  << EOF | tee -a;
cd $VAL_TRREMOTEDIR2
pwd
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_NOM_ARCHIVO2_0}
mget ${VAL_NOM_ARCHIVO2_1}
bye
EOF

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

ETAPA=3
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 3: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "3" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 3: Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: ${VAL_RUTA}/python/read_excel_carga_hive.py" 2>&1 &>> $VAL_LOG
echo "Archivo: $VAL_RUTA_ARCHIVO_1_2" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$VAL_TABLA_T" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--master $VAL_MASTER \
${VAL_RUTA}/python/read_excel_carga_hive.py \
--rutain=$VAL_RUTA_ARCHIVO_1_2 \
--tablaout=$HIVEDB.$VAL_TABLA_T \
--tipo=$VAL_TIPO_CARGA 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Unrecognized option|Traceback|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark read_excel_carga_hive.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark read_excel_carga_hive.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

cat $VAL_LOG|grep "Total registros" |grep "$HIVEDB.$VAL_TABLA_T"

ETAPA=4
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 3 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='4' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 4: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "4" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 4: Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: ${VAL_RUTA}/python/read_excel_carga_hive.py" 2>&1 &>> $VAL_LOG
echo "Archivo: $VAL_RUTA_ARCHIVO_1_0" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$VAL_TABLA_RDR" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--master $VAL_MASTER \
${VAL_RUTA}/python/read_excel_carga_hive.py \
--rutain="$VAL_RUTA_ARCHIVO_1_0" \
--tablaout=$HIVEDB.$VAL_TABLA_RDR \
--tipo=$VAL_TIPO_CARGA 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Unrecognized option|Traceback|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark read_excel_carga_hive.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark read_excel_carga_hive.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

cat $VAL_LOG|grep "Total registros" |grep "$HIVEDB.$VAL_TABLA_RDR"

ETAPA=5
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 4 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='5' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 5: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "5" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 5: Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: ${VAL_RUTA}/python/read_excel_carga_hive.py" 2>&1 &>> $VAL_LOG
echo "Archivo: $VAL_RUTA_ARCHIVO_2_1" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$VAL_TABLA_TC" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--master $VAL_MASTER \
${VAL_RUTA}/python/read_excel_carga_hive.py \
--rutain=$VAL_RUTA_ARCHIVO_2_1 \
--tablaout=$HIVEDB.$VAL_TABLA_TC \
--tipo=$VAL_TIPO_CARGA 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Unrecognized option|Traceback|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark read_excel_carga_hive.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark read_excel_carga_hive.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

cat $VAL_LOG|grep "Total registros" |grep "$HIVEDB.$VAL_TABLA_TC"

ETAPA=6
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 5 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='6' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 6: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "6" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 6: Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: ${VAL_RUTA}/python/read_excel_carga_hive.py" 2>&1 &>> $VAL_LOG
echo "Archivo: $VAL_RUTA_ARCHIVO_2_0" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$VAL_TABLA_UO" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--master $VAL_MASTER \
${VAL_RUTA}/python/read_excel_carga_hive.py \
--rutain=$VAL_RUTA_ARCHIVO_2_0 \
--tablaout=$HIVEDB.$VAL_TABLA_UO \
--tipo=$VAL_TIPO_CARGA 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Unrecognized option|Traceback|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark read_excel_carga_hive.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark read_excel_carga_hive.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

cat $VAL_LOG|grep "Total registros" |grep "$HIVEDB.$VAL_TABLA_UO"

ETAPA=7
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 6 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='7' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 7: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "7" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 7: Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: ${VAL_RUTA}/python/read_excel_carga_hive.py" 2>&1 &>> $VAL_LOG
echo "Archivo: $VAL_RUTA_ARCHIVO_1_3" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$VAL_TABLA_CANAL" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--master $VAL_MASTER \
${VAL_RUTA}/python/read_excel_carga_hive.py \
--rutain=$VAL_RUTA_ARCHIVO_1_3 \
--tablaout=$HIVEDB.$VAL_TABLA_CANAL \
--tipo=$VAL_TIPO_CARGA 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Unrecognized option|Traceback|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark read_excel_carga_hive.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark read_excel_carga_hive.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

cat $VAL_LOG|grep "Total registros" |grep "$HIVEDB.$VAL_TABLA_CANAL"

ETAPA=8
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 7 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='8' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 8: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "8" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 8: Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: ${VAL_RUTA}/python/read_excel_carga_hive.py" 2>&1 &>> $VAL_LOG
echo "Archivo: $VAL_RUTA_ARCHIVO_1_4" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$VAL_TABLA_SEG" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--master $VAL_MASTER \
${VAL_RUTA}/python/read_excel_carga_hive.py \
--rutain=$VAL_RUTA_ARCHIVO_1_4 \
--tablaout=$HIVEDB.$VAL_TABLA_SEG \
--tipo=$VAL_TIPO_CARGA 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Unrecognized option|Traceback|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark read_excel_carga_hive.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark read_excel_carga_hive.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

cat $VAL_LOG|grep "Total registros" |grep "$HIVEDB.$VAL_TABLA_SEG"

ETAPA=9
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 8 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='9' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 9: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "9" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 9: Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Proceso: ${VAL_RUTA}/python/read_excel_carga_hive.py" 2>&1 &>> $VAL_LOG
echo "Archivo: $VAL_RUTA_ARCHIVO_MP" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$VAL_TABLA_CST" 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--master $VAL_MASTER \
${VAL_RUTA}/python/read_excel_carga_hive.py \
--rutain=$VAL_RUTA_ARCHIVO_MP \
--tablaout=$HIVEDB.$VAL_TABLA_CST \
--tipo=$VAL_TIPO_CARGA 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Unrecognized option|Traceback|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark read_excel_carga_hive.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark read_excel_carga_hive.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

cat $VAL_LOG|grep "Total registros" |grep "$HIVEDB.$VAL_TABLA_CST"

ETAPA=10
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 9 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='10' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#HACE EL LLAMADO AL HQL QUE REALIZA LOS CRUCES PARA GENERAR LA INFORMACION EN LA TABLA FINAL OTC_T_TERMINALES_SIMCARDS
if [ "$ETAPA" = "10" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 10: Ejecuta subproceso PySpark carga_otc_t_terminales_simcards_1.py ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "Fecha meses atras 2:     $VAL_MESES_ATRAS2" 2>&1 &>> $VAL_LOG
echo "Fecha Fin:               $VAL_DIA_UNO" 2>&1 &>> $VAL_LOG

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
--queue $VAL_QUEUE \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/carga_otc_t_terminales_simcards_1.py \
--ventidad=$ENTIDAD \
--vhivebd=$HIVEDB \
--vfecha_fin=$VAL_DIA_UNO \
--vfecha_meses_atras2=$VAL_MESES_ATRAS2 2>&1 &>> $VAL_LOG

error_spark=`egrep 'Traceback|error: argument|invalid syntax|An error occurred|Caused by:|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|ImportError|SyntaxError' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark carga_otc_t_terminales_simcards_1.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark carga_otc_t_terminales_simcards_1.py ====" 2>&1 &>> $VAL_LOG
exit 1
fi

ETAPA=11
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 10 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='11' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#HACE EL LLAMADO AL HQL QUE REALIZA LOS CRUCES PARA GENERAR LA INFORMACION EN LA TABLA FINAL OTC_T_TERMINALES_SIMCARDS
if [ "$ETAPA" = "11" ]; then
echo "=======================================================================================================" 2>&1 &>> $VAL_LOG
echo "==== ETAPA 11: Ejecuta subproceso PySpark carga_otc_t_terminales_simcards_2.py ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
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
--queue $VAL_QUEUE \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/carga_otc_t_terminales_simcards_2.py \
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
echo "==== OK - La ejecucion del archivo spark carga_otc_t_terminales_simcards_2.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark carga_otc_t_terminales_simcards_2.py ====" 2>&1 &>> $VAL_LOG
exit 1
fi

#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 11 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

echo "==== Finaliza ejecucion del proceso BI CS Terminales Simcards ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
