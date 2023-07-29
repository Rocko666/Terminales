#########################################################################################################
# NOMBRE: SH_TERMINALES_SIMCARDS.sh     		     	      								            #
# DESCRIPCION:																							#
#  Shell principal que realiza el proceso para generar la informacio de terminales simcards			#
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2021-12-14   																			#
# PARAMETROS DEL SHELL                            													    #
# VAL_FECHA_EJEC=${1} 		Fecha de ejecucion de proceso en formato  YYYYMMDD                          #
# VAL_COLA_EJECUCION=${2} 	Cola utilizada para ejecutar el proceso                                     #
# VAL_CADENA_JDBC=${3} 		Cadena de conexiÃ³n a Hive utilizada para el proceso                         #
# VAL_RUTA=${4} 			Ruta donde se encuentran los objetos del proceso                            #
# VAL_USUARIO=${5} 			Usuario utilizado para ejecuciÃ³n del proceso                                #
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
# 2022-01-10    Karina Castro	Se agrega el llamado a las shell sqoop export, a las shells sqoop import#
#                               a los paquetes en oracle y al HQL que carg al ainformacion en Hive.		#
#########################################################################################################
##########################################################################
# MODIFICACIONES														 #
# FECHA  		AUTOR     		DESCRIPCION MOTIVO						 #
# 2023-03-15	Alexandra Macas	Se migra importacion a spark             #								
##########################################################################
##############
# VARIABLES #
##############

ENTIDAD=SHTRMNLSMCRDS0010

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_FECHA_EJEC=$1
VAL_RUTA=$2
ETAPA=$3

#1. PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_NOM_JAR_ORC_11=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_11';"`
VAL_RUTA_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_IMP_SPARK';"`
VAL_NOM_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_IMP_SPARK';"`

VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USUARIO=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`

#2. PARAMETROS DE ENTIDAD
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_FTP_PUERTO1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO1';"` #9559
VAL_FTP_PUERTO2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO2';"` #9557
VAL_FTP_USER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_USER';"` #ftp_contabilidad
VAL_FTP_HOSTNAME=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_HOSTNAME';"` #10.112.47.36
VAL_FTP_PASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PASS';"` #Telefonica.2018&
VAL_FTP_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_RUTA';"` #/
VAL_NOM_ARCHIVO1_0=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_0';"` #LISTADO_RUC_DAS_RETAIL.xlsx
VAL_NOM_ARCHIVO1_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_1';"` #CONCEPTOS_FACT_ADICIONALES.xlsx
VAL_NOM_ARCHIVO1_2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_2';"` #100 Nueva Categoria.xlsx
VAL_NOM_ARCHIVO2_0=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO2_0';"` #USUARIOS_CANAL_ONLINE.xlsx
VAL_NOM_ARCHIVO2_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO2_1';"` #TIPO_CANAL.xlsx
VAL_BASE_DATOS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_BASE_DATOS';"` #db_cs_terminales
VAL_TABLA_TC=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_TC';"` #OTC_T_CATALOGO_TIPO_CANAL
VAL_TABLA_RDR=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_RDR';"` #OTC_T_CATALOGO_RUC_DAS_RETAIL
VAL_TABLA_T=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_T';"` #OTC_T_CATALOGO_TERMINALES
VAL_TABLA_UO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_UO';"` #OTC_T_CATALOGO_CANAL_ONLINE
VAL_TABLA_ZTFI=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_ZTFI';"` #OTC_T_CONCEPTOS_FACT_ADICION
VAL_USUARIO_BACKOFF=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO_BACKOFF';"` #NAG00029,"NAINJA02","NALAJA01","NAUIJA61","NAUIJA62","NAUIJA65","NAUIJA66","NAUIJA67","NAUIJA82","internal"
VAL_USUARIO4=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO4';"` #NA002828
VAL_USUARIO_FINAL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO_FINAL';"` #NA400413,"NA002132","NA1000206","NA002152","MFSALAZAR","NA400406","NA002420"
VAL_MESES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MESES';"` #2
TDUSER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDUSER';"` #RDB_REPORTES
TDPASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPASS';"` #TelfEcu2017
TDHOST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDHOST';"` #proxfulldg1.otecel.com.ec
TDPORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPORT';"` #7594
TDDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDDB';"` #tomstby.otecel.com.ec
TDTABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDTABLE';"` #OTC_NC_TERM_SIMC_FINAL
TDUSER1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDUSER1';"` #RBM_REPORTES
TDPASS1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPASS1';"` #TelfEcu2017
TDHOST1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDHOST1';"` #proxfulldg2.otecel.com.ec
TDPORT1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPORT1';"` #7594
TDDB1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDDB1';"` #norstby.otecel.com.ec

VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_CORES_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORES_EXECUTORS';"`
VAL_TIPO_CARGA_O=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA_O';"`
HIVEDB_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB_TMP';"` #db_temporales

VAL_JDBCURL_2=jdbc:oracle:thin:@//$TDHOST:$TDPORT/$TDDB

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FEC_AYER=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y%m%d"`
VAL_DIA_UNO=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m01"`
VAL_FECHA_FORMATO=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y-%m-%d"`
VAL_MES=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m"`
VAL_OVERRIDE_INI=`date -d "${VAL_DIA_UNO} -15 day"  +"%Y%m%d"`
VAL_OVERRIDE_FIN=`date -d "${VAL_FECHA_EJEC} +5 day"  +"%Y%m%d"`
VAL_SYSDATE=$(date)
VAL_MESES_ATRAS=`date -d "${VAL_SYSDATE} -${VAL_MESES} month"  +"%Y%m%d"`
VAL_FECHA_FORM_INI=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y-%m-01"`
VAL_FECHA_FORMATO_INI=$VAL_FECHA_FORM_INI" 00:00:00"
VAL_FECHA_FORMATO_FIN=$VAL_FECHA_FORMATO" 23:59:59"
VAL_YEAR=`echo $VAL_MESES_ATRAS | cut -c1-4`
VAL_MONTH=`echo $VAL_MESES_ATRAS | cut -c5-6`
VAL_DAY=`echo $VAL_MESES_ATRAS | cut -c7-8`
VAL_2_MESES=$VAL_YEAR"-"$VAL_MONTH"-"$VAL_DAY" 00:00:00"
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/logs/SH_TERMINALES_SIMCARDS_$VAL_DIA$VAL_HORA.log
VAL_RUTA_ARCHIVO=$VAL_RUTA/input
VAL_NOM_ARCHIVO1_2_N=100_Nueva_Categoria.xlsx

#VALIDACION DE PARAMETROS INICIALES
if [ -z "$ENTIDAD" ] || [ -z "$VAL_FECHA_EJEC" ] || [ -z "$VAL_COLA_EJECUCION" ] || [ -z "$VAL_CADENA_JDBC" ] || [ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_USUARIO" ] || [ -z "$ETAPA" ] || [ -z "$VAL_FTP_PUERTO1" ] || [ -z "$VAL_FTP_PUERTO2" ] || [ -z "$VAL_FTP_USER" ] || 
	[ -z "$VAL_FTP_HOSTNAME" ] || [ -z "$VAL_FTP_PASS" ] || [ -z "$VAL_FTP_RUTA" ] || [ -z "$VAL_NOM_ARCHIVO1_0" ] || [ -z "$VAL_NOM_ARCHIVO1_1" ] || 
	[ -z "$VAL_NOM_ARCHIVO1_2" ] || [ -z "$VAL_NOM_ARCHIVO2_0" ] || [ -z "$VAL_NOM_ARCHIVO2_1" ] || [ -z "$VAL_MASTER" ] || [ -z "$VAL_DRIVER_MEMORY" ] || 
	[ -z "$VAL_EXECUTOR_MEMORY" ] || [ -z "$VAL_CORES_EXECUTORS" ] || [ -z "$VAL_TIPO_CARGA_O" ] || [ -z "$VAL_BASE_DATOS" ] || [ -z "$VAL_TABLA_TC" ] || 
	[ -z "$VAL_TABLA_RDR" ] || [ -z "$VAL_TABLA_T" ] || [ -z "$VAL_TABLA_UO" ] || [ -z "$VAL_TABLA_ZTFI" ] || [ -z "$VAL_USUARIO_BACKOFF" ] || 
	[ -z "$VAL_USUARIO4" ] || [ -z "$VAL_USUARIO_FINAL" ] || [ -z "$VAL_MESES" ] || [ -z "$TDUSER" ] || [ -z "$TDPASS" ] || [ -z "$TDHOST" ] || 
	[ -z "$TDPORT" ] || [ -z "$TDDB" ] || [ -z "$TDTABLE" ] || [ -z "$TDUSER1" ] || [ -z "$TDPASS1" ] || [ -z "$TDHOST1" ] || [ -z "$TDPORT1" ] || 
	[ -z "$TDDB1" ] || [ -z "$HIVEDB_TMP" ] || [ -z "$VAL_JDBCURL_2" ] || [ -z "$VAL_NOM_ARCHIVO1_2_N" ] || [ -z "$VAL_RUTA_ARCHIVO" ] || [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo" >> $VAL_LOG
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion del proceso BI CS Terminales Simcards  ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" >> $VAL_LOG
echo "Fecha Ejecucion: $VAL_FECHA_EJEC" >> $VAL_LOG
echo "Fecha Dia Caido: $VAL_FEC_AYER" >> $VAL_LOG

#################
# 	ETAPA:1		#
#################
#PASO 1: REALIZA LA TRANSFERENCIA DE LOS 2 PRIMEROS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "1" ]; then
echo "==== Realiza la transferencia de los 2 primeros archivos en formato Excel desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Servidor: $VAL_FTP_HOSTNAME" >> $VAL_LOG
echo "Puerto: $VAL_FTP_PUERTO1" >> $VAL_LOG
echo "Ruta: $VAL_FTP_RUTA" >> $VAL_LOG
echo "Archivo 1: $VAL_NOM_ARCHIVO1_0" >> $VAL_LOG
echo "Archivo 2: $VAL_NOM_ARCHIVO1_1" >> $VAL_LOG
echo "Archivo 3: $VAL_NOM_ARCHIVO1_2" >> $VAL_LOG
#ELIMINA LOS ARCHIVOS EXCEL DE RUTA INPUT
rm -f ${VAL_RUTA}/input/*

ftp -inv $VAL_FTP_HOSTNAME $VAL_FTP_PUERTO1 <<EOF >> $VAL_LOG
user $VAL_FTP_USER $VAL_FTP_PASS
bin
pwd
cd ${VAL_FTP_RUTA}
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_NOM_ARCHIVO1_0}
mget ${VAL_NOM_ARCHIVO1_1}
mget ${VAL_NOM_ARCHIVO1_2}
bye
EOF

#VALIDA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A BIGDATA
echo "==== Valida la transferencia de los 2 primeros archivos desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_trnsf=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
if [ $error_trnsf -eq 0 ];then
	echo "==== OK - La transferencia de los 2 primeros archivos desde el servidor FTP se realiza con EXITO ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la transferencia de los 2 primeros archivos desde el servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
    exit 1
fi

#REALIZA COPIA DEL ARCHIVO 100 Nueva Categoria PARA ELIMINAR ESPACIOS EN EL NOMBRE
echo "==== Realiza copia del archivo 100 Nueva Categoria para eliminar espacios en el nombre ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cd $VAL_RUTA/input
cp 100*Nueva*Categoria.xlsx $VAL_NOM_ARCHIVO1_2_N

echo "==== Realiza la transferencia de los otros 2 archivos en formato Excel desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Servidor: $VAL_FTP_HOSTNAME" >> $VAL_LOG
echo "Puerto: $VAL_FTP_PUERTO2" >> $VAL_LOG
echo "Ruta: $VAL_FTP_RUTA" >> $VAL_LOG
echo "Archivo 4: $VAL_NOM_ARCHIVO2_0" >> $VAL_LOG
echo "Archivo 5: $VAL_NOM_ARCHIVO2_1" >> $VAL_LOG

ftp -inv $VAL_FTP_HOSTNAME $VAL_FTP_PUERTO2 <<EOF >> $VAL_LOG
user $VAL_FTP_USER $VAL_FTP_PASS
bin
cd ${VAL_FTP_RUTA}
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_NOM_ARCHIVO2_0}
mget ${VAL_NOM_ARCHIVO2_1}
bye
EOF

#VALIDA LA TRANSFERENCIA DEL ARCHIVO DESDE EL SERVIDOR FTP A BIGDATA
echo "==== Valida la transferencia de los otros 2 archivos desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_trnsf=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
if [ $error_trnsf -eq 0 ]; then
	echo "==== OK - La transferencia de los otros 2 archivos desde el servidor FTP se realiza con EXITO ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
else
	echo "==== ERROR: - En la transferencia de los otros 2 archivos desde el servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
   	 exit 1
fi

ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#################
# 	ETAPA:2		#
#################

#PASO 2: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA LOS CATALOGOS DE EXCEL A HIVE
if [ "$ETAPA" = "2" ]; then
echo "==== Ejecuta archivo spark carga_excel_a_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_CORES_EXECUTORS \
${VAL_RUTA}/python/carga_excel_a_hive.py \
-base_datos $VAL_BASE_DATOS \
-tabla_1 $VAL_TABLA_T \
-tabla_2 $VAL_TABLA_RDR \
-tabla_3 $VAL_TABLA_TC \
-tabla_4 $VAL_TABLA_UO \
-tabla_5 $VAL_TABLA_ZTFI \
-ruta_archivo $VAL_RUTA_ARCHIVO \
-archivo_csv_1 $VAL_NOM_ARCHIVO1_2_N \
-archivo_csv_2 $VAL_NOM_ARCHIVO1_0 \
-archivo_csv_3 $VAL_NOM_ARCHIVO2_1 \
-archivo_csv_4 $VAL_NOM_ARCHIVO2_0 \
-archivo_csv_5 $VAL_NOM_ARCHIVO1_1 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'ValueError:|ERROR:|SyntaxError: invalid syntax|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

ETAPA=3
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#################
# 	ETAPA:3		#
#################

#HACE EL LLAMADO A LA SHELL  OTC_T_CTL_POS_USR_NC QUE EXPORTA INFORMACION DE HIVE A ORACLE
if [ "$ETAPA" = "3" ]; then
echo "==== Ejecuta spark ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

#SE BORRAN LOS DATOS EXISTENTS ANTES DE INSERTAR LOS NUEVOS
sqlplus $TDUSER/$TDPASS@$TDHOST:$TDPORT/$TDDB <<EOF 
TRUNCATE TABLE $TDUSER.OTC_T_CTL_POS_USR_NC_TMP;
commit;
EOF

TD_DB=$TDUSER.OTC_T_CTL_POS_USR_NC_TMP #OTC_T_CTL_POS_USR_NC_TMP_CL  #--cambiar OTC_T_CTL_POS_USR_NC_TMP
echo "==== INICIA PROCESO SPARK"

#SPARK DE HIVE
$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_CORES_EXECUTORS \
--conf spark.default.parallelism=170 \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA/python/export_otc_t_ctl_pos_usr_nc.py \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL_2 \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vdbtable=$TD_DB \
--vtipocarga="append" \
--ventidad=$ENTIDAD \
--vhivebd=$HIVEDB_TMP 2>&1 &>> $VAL_LOG

error_spark=`egrep -i 'An error occurred|Caused by:|SyntaxError: invalid syntax|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ]; then
	echo "==== OK - La ejecucion del archivo spark export_otc_t_ctl_pos_usr_nc.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
else
	echo "==== ERROR: - otcx_ En la ejecucion del archivo spark export_otc_t_ctl_pos_usr_nc.py ====" >> $VAL_LOG
	exit 1
fi

ETAPA=4
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 3 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='4' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#################
# 	ETAPA:4		# 
#################

#HACE EL LLAMADO A LA SHELL  OTC_T_ZTFI_TABLA QUE EXPORTA INFORMACION DE HIVE A ORACLE 
if [ "$ETAPA" = "4" ]; then
echo "==== TRUNCATE DE LA TABLA DE ORACLE OTC_T_ZTFI_TABLA ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

sqlplus $TDUSER/$TDPASS@$TDHOST:$TDPORT/$TDDB <<EOF 
TRUNCATE TABLE $TDUSER.OTC_T_ZTFI_TABLA;
commit;
EOF

TD_DB_2=$TDUSER.OTC_T_ZTFI_TABLA
echo "==== Calcula fecha maxima de la particion de la tabla OTC_T_ZTFI_TABLA ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
fecha_max=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT max(fecha_proceso) FROM db_facturacion.otc_t_ztfi_tabla;")
echo "La fecha maxima es:$fecha_max" >> $VAL_LOG
echo "==== Ejecuta Shell spark Export OTC_T_ZTFI_TABLA.sh - Revisar log  ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

#SPARK DE HIVE
$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_CORES_EXECUTORS \
--conf spark.default.parallelism=170 \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA/python/export_otc_t_ztfi_tabla.py \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL_2 \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vdbtable=$TD_DB_2 \
--vtipocarga="append" \
--ventidad=$ENTIDAD \
--vhivebd=$HIVEDB_TMP \
--vfecha_max=$fecha_max 2>&1 &>> $VAL_LOG

error_spark=`egrep -i 'An error occurred|Caused by:|SyntaxError: invalid syntax|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark export_otc_t_ztfi_tabla.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
else
	echo "==== ERROR: - otcx_ En la ejecucion del archivo spark export_otc_t_ztfi_tabla.py ====" >> $VAL_LOG
	exit 1
fi

ETAPA=5
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 4 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='5' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#################
# 	ETAPA:5		# 
#################

#EJECUTA PAQUETES EN ORACLE
if [ "$ETAPA" = "5" ]; then
echo "==== Ejecuta paquetes en Oracle con los siguientes parametros ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Fecha Inicio: $VAL_DIA_UNO" >> $VAL_LOG
echo "Fecha Fin: $VAL_FEC_AYER" >> $VAL_LOG
echo "Fecha Inicio Formato: $VAL_FECHA_FORMATO_INI" >> $VAL_LOG
echo "Fecha Fin Formato: $VAL_FECHA_FORMATO_FIN" >> $VAL_LOG
echo "Fecha 2 Meses Atras Formato: $VAL_2_MESES" >> $VAL_LOG
echo "==== Ejecuta paquete OTC_K_BILLDETAILS_MES_FILTRO en RBM_REPORTES===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
sql_f="exec OTC_K_BILLDETAILS_MES_FILTRO.OTC_P_BILLDETAILS_MES_FILTRO(P_FEC_INI_FORMATO => '$VAL_FECHA_FORMATO_INI',P_FEC_FIN_FORMATO => '$VAL_FECHA_FORMATO_FIN');"
VAL_MENSAJE=`echo -e "SET PAGESIZE 0\n SET FEEDBACK OFF\n $sql_f" | sqlplus -S $TDUSER1/$TDPASS1@$TDHOST1:$TDPORT1/$TDDB1`
echo "El mensaje es: $VAL_MENSAJE" >> $VAL_LOG

#VALIDA EJECUCION DEL PAQUETE OTC_K_BILLDETAILS_MES_FILTRO
echo "==== Valida ejecucion del paquete OTC_K_BILLDETAILS_MES_FILTRO ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_crea=`egrep 'invalid|FAILED:|ERROR:|ORA-02063|ORA-06512' $VAL_LOG | wc -l`
	if [ $error_crea -eq 0 ];then
		echo "==== OK - La ejecucion del paquete OTC_K_BILLDETAILS_MES_FILTRO es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
		else
		echo "==== ERROR: - En la ejecucion del paquete OTC_K_BILLDETAILS_MES_FILTRO ====" >> $VAL_LOG
		exit 1
	fi

echo "==== Ejecuta paquete OTC_K_CAPA_SMTCA_TERM_SIMC en RDB_REPORTES ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
sql_f="exec OTC_K_CAPA_SMTCA_TERM_SIMC.OTC_P_PRINCIPAL(P_FECHA_INI => '$VAL_DIA_UNO',P_FECHA_FIN => '$VAL_FEC_AYER',P_FEC_INI_FORMATO => '$VAL_FECHA_FORMATO_INI',P_FEC_FIN_FORMATO => '$VAL_FECHA_FORMATO_FIN',P_FEC_2_MESES => '$VAL_2_MESES');"
VAL_MENSAJE=`echo -e "SET PAGESIZE 0\n SET FEEDBACK OFF\n $sql_f" | sqlplus -S $TDUSER/$TDPASS@$TDHOST:$TDPORT/$TDDB`
echo "El mensaje es: $VAL_MENSAJE" >> $VAL_LOG

#VALIDA EJECUCION DEL PAQUETE OTC_K_CAPA_SMTCA_TERM_SIMC
echo "==== Valida ejecucion del paquete OTC_K_CAPA_SMTCA_TERM_SIMC ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_crea=`egrep 'invalid|FAILED:|ERROR:|ORA-02063|ORA-06512' $VAL_LOG | wc -l`
	if [ $error_crea -eq 0 ];then
		echo "==== OK - La ejecucion del paquete OTC_K_CAPA_SMTCA_TERM_SIMC es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
		else
		echo "==== ERROR: - En la ejecucion del paquete OTC_K_CAPA_SMTCA_TERM_SIMC ====" >> $VAL_LOG
		exit 1
	fi

ETAPA=6
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 5 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='6' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#################
# 	ETAPA:6		# 
#################

#HACE EL LLAMADO A LA SHELL  OTC_NC_TERMINALES_SIMCARDS_INT QUE EXTRAE INFORMACION DE ORACLE
if [ "$ETAPA" = "6" ]; then
echo "==== Ejecuta Shell spark  Import - Revisar log ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

#SPARK DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL_2 \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vhivebd=$VAL_BASE_DATOS \
--vtablahive="OTC_NC_TERMINALES_SIMCARDS_INT" \
--vtipocarga=$VAL_TIPO_CARGA_O \
--vquery1="" \
--vfilesql=$VAL_RUTA/sql/otc_nc_terminales_simcards_int.sql 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep -i 'An error occurred|Caused by:|UnicodeEncodeError:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error |unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark_import.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
else
	echo "==== ERROR: - En la ejecucion del archivo spark_import.py ====" >> $VAL_LOG
	exit 1
fi
	
ETAPA=7
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 6 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='7' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#################
# 	ETAPA:7		# 
#################

#HACE EL LLAMADO A LA SHELL  OTC_NC_DISTRIBUIDORES QUE EXTRAE INFORMACION DE ORACLE
if [ "$ETAPA" = "7" ]; then
echo "==== Ejecuta Shell spark Import -  ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

#SPARK DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL_2 \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vhivebd=$VAL_BASE_DATOS \
--vtablahive="OTC_NC_DISTRIBUIDORES" \
--vtipocarga=$VAL_TIPO_CARGA_O \
--vquery1="" \
--vfilesql=$VAL_RUTA/sql/otc_nc_distribuciones.sql 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep -i 'An error occurred|Caused by:|UnicodeEncodeError:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error |unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark_import.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
else
	echo "==== ERROR: - En la ejecucion del archivo spark_import.py ====" >> $VAL_LOG
	exit 1
fi

ETAPA=8
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 7 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='8' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
set -e
#################
# 	ETAPA:8		# 
#################
#HACE EL LLAMADO AL HQL QUE REALIZA LOS CRUCES PARA GENERAR LA INFORMACION EN LA TABLA FINAL OTC_T_TERMINALES_SIMCARDS
if [ "$ETAPA" = "8" ]; then

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Inicia la ejecucion de la etapa: $ETAPA" >> $VAL_LOG
###################################################################################################################
$VAL_RUTA_SPARK \
--conf spark.rpc.message.maxSize=128 \
--conf spark.default.parallelism=170 \
--conf spark.broadcast.checksum=true \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors 4 \
--executor-cores 4 \
$VAL_RUTA/python/otc_t_terminales_simcards_1.py \
--vSEntidad=$ENTIDAD \
--vIEtapa=$ETAPA \
--vSChemaTmp=$HIVEDB_TMP \
--vSChema=$VAL_BASE_DATOS \
--vSQueue=$VAL_COLA_EJECUCION \
--FECHA_HOY=$VAL_FECHA_EJEC \
--ANIO=$VAL_YEAR \
--MES=$VAL_MONTH \
--FECHA_INICIO=$VAL_DIA_UNO \
--FECHA_FIN_DATE=$VAL_FECHA_FORMATO \
--BACK_OFFICE=$VAL_USUARIO_BACKOFF \
--USUARIO4=$VAL_USUARIO4 \
--FECHA_FIN=$VAL_FEC_AYER \
--USUARIO_FINAL=$VAL_USUARIO_FINAL \
--FECHA_OVERRIDE=$VAL_OVERRIDE_INI \
--FECHA_OVERRIDE_FIN=$VAL_OVERRIDE_FIN 2>&1 &>> $VAL_LOG

#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 8 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
ETAPA=9
`mysql -N  <<<"update params set valor='$ETAPA' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#################
# 	ETAPA:9		# 
#################
if [ "$ETAPA" = "9" ]; then
###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Inicia la ejecucion de la etapa: $ETAPA" >> $VAL_LOG
###################################################################################################################
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
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors 4 \
--executor-cores 4 \
$VAL_RUTA/python/otc_t_terminales_simcards_2.py \
--vSEntidad=$ENTIDAD \
--vIEtapa=$ETAPA \
--vSChemaTmp=$HIVEDB_TMP \
--vSChema=$VAL_BASE_DATOS \
--vSTable=otc_t_terminales_simcards \
--vSQueue=$VAL_COLA_EJECUCION \
--FECHA_HOY=$VAL_FECHA_EJEC \
--ANIO=$VAL_YEAR \
--MES=$VAL_MES \
--FECHA_INICIO=$VAL_DIA_UNO \
--FECHA_FIN_DATE=$VAL_FECHA_FORMATO \
--BACK_OFFICE=$VAL_USUARIO_BACKOFF \
--USUARIO4=$VAL_USUARIO4 \
--FECHA_FIN=$VAL_FEC_AYER \
--USUARIO_FINAL=$VAL_USUARIO_FINAL \
--FECHA_OVERRIDE=$VAL_OVERRIDE_INI \
--FECHA_OVERRIDE_FIN=$VAL_OVERRIDE_FIN \
--vBeelineJDBC=$VAL_CADENA_JDBC \
--vBeelineUser=$VAL_USUARIO 2>&1 &>> $VAL_LOG

#VALIDACION DE REGISTROS DE LA TABLA
reg_nuevos_02=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
--showHeader=false --outputformat=tsv2 -e \
"select count(1) from $VAL_BASE_DATOS.otc_t_terminales_simcards WHERE p_fecha_factura= ${VAL_FEC_AYER} ;") &>>$VAL_LOG
echo "Numero registros tabla $VAL_BASE_DATOS.otc_t_terminales_simcards="$reg_nuevos_02 &>> $VAL_LOG
#
if [ $reg_nuevos_02 -ne 0 ];then
	echo "Cantidad Registros dia actual: "$reg_nuevos_02 &>> $VAL_LOG	
	
else
	echo "****No Existe Datos****"
	error=3
	echo "ERROR en la ETAPA ${ETAPA}" &>> $VAL_LOG
	exit $error;
fi


#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 9 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
fi
		
echo "==== Finaliza ejecucion del proceso BI CS Terminales Simcards ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
