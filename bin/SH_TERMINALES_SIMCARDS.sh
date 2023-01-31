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
# MODIFICACIONES																						#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
#########################################################################################################
##############
# VARIABLES #
##############
ENTIDAD=D_SHTRMNLSMCRDS0010

#version=1.2.1000.2.6.4.0-91
version=1.2.1000.2.6.5.0-292
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

#PARAMETROS DEFINIDOS EN LA TABLA params_des
VAL_FECHA_EJEC=$1
#VAL_RUTA=$2
VAL_RUTA=/home/nae105215/cp_terminales_simcards
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_USUARIO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO';"`
ETAPA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_FTP_PUERTO1=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO1';"`
VAL_FTP_PUERTO2=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO2';"`
VAL_FTP_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_USER';"`
VAL_FTP_HOSTNAME=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_HOSTNAME';"`
VAL_FTP_PASS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PASS';"`
VAL_FTP_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_RUTA';"`
VAL_FTP_PUERTO_MP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PUERTO_MP';"`
VAL_FTP_USER_MP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_USER_MP';"`
VAL_FTP_HOSTNAME_MP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_HOSTNAME_MP';"`
VAL_FTP_PASS_MP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_PASS_MP';"`
VAL_FTP_RUTA_MP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FTP_RUTA_MP';"`
VAL_NOM_ARCHIVO1_0=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_0';"`
VAL_NOM_ARCHIVO1_2=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_2';"`
VAL_NOM_ARCHIVO1_3=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_3';"`
VAL_NOM_ARCHIVO1_4=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO1_4';"`
VAL_NOM_ARCHIVO_MP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO_MP';"`
VAL_NOM_ARCHIVO2_0=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO2_0';"`
VAL_NOM_ARCHIVO2_1=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO2_1';"`
VAL_BASE_DATOS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_BASE_DATOS';"`
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
VAL_NOM_ARCHIVO_PREVIO=EXT_TERMINALES.txt

#VALIDACION DE PARAMETROS INICIALES
if [ -z "$ENTIDAD" ] || [ -z "$VAL_FECHA_EJEC" ] || [ -z "$VAL_COLA_EJECUCION" ] || [ -z "$VAL_CADENA_JDBC" ] || [ -z "$VAL_RUTA" ] || [ -z "$VAL_USUARIO" ] || [ -z "$ETAPA" ] || [ -z "$VAL_FTP_PUERTO1" ] || [ -z "$VAL_FTP_PUERTO2" ] || [ -z "$VAL_FTP_USER" ] || [ -z "$VAL_FTP_HOSTNAME" ] || [ -z "$VAL_FTP_PASS" ] || [ -z "$VAL_FTP_RUTA" ] || [ -z "$VAL_FTP_PUERTO_MP" ] || [ -z "$VAL_FTP_USER_MP" ] || [ -z "$VAL_FTP_HOSTNAME_MP" ] || [ -z "$VAL_FTP_PASS_MP" ] || [ -z "$VAL_FTP_RUTA_MP" ] || [ -z "$VAL_NOM_ARCHIVO1_0" ] || [ -z "$VAL_NOM_ARCHIVO1_2" ] || [ -z "$VAL_NOM_ARCHIVO1_3" ] || [ -z "$VAL_NOM_ARCHIVO1_4" ] || [ -z "$VAL_NOM_ARCHIVO_MP" ] || [ -z "$VAL_NOM_ARCHIVO2_0" ] || [ -z "$VAL_NOM_ARCHIVO2_1" ] || [ -z "$VAL_BASE_DATOS" ] || [ -z "$VAL_TABLA_TC" ] || [ -z "$VAL_TABLA_RDR" ] || [ -z "$VAL_TABLA_T" ] || [ -z "$VAL_TABLA_UO" ] || [ -z "$VAL_TABLA_CANAL" ] || [ -z "$VAL_TABLA_SEG" ] || [ -z "$VAL_TABLA_CST" ] || [ -z "$VAL_USUARIO4" ] || [ -z "$VAL_USUARIO_FINAL" ] || [ -z "$VAL_MESES" ] || [ -z "$VAL_MESES1" ] || [ -z "$VAL_MESES2" ] || [ -z "$VAL_FTP_PUERTO_OUT" ] || [ -z "$VAL_FTP_USER_OUT" ] || [ -z "$VAL_FTP_HOSTNAME_OUT" ] || [ -z "$VAL_FTP_PASS_OUT" ] || [ -z "$VAL_FTP_RUTA_OUT" ] || [ -z "$VAL_NOM_ARCHIVO" ] || [ -z "$VAL_TIPO_CARGA" ] || [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion del proceso BI CS Terminales Simcards  ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" >> $VAL_LOG
echo "Fecha Inicio: $VAL_FECHA_INI" >> $VAL_LOG
echo "Fecha Fin: $VAL_DIA_UNO" >> $VAL_LOG

#PASO 1: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "1" ]; then
echo "==== Realiza la transferencia de los archivos en formato Excel desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Servidor: $VAL_FTP_HOSTNAME" >> $VAL_LOG
echo "Puerto: $VAL_FTP_PUERTO1" >> $VAL_LOG
echo "Ruta: $VAL_FTP_RUTA" >> $VAL_LOG
#ELIMINA LOS ARCHIVOS EXCEL DE RUTA INPUT
rm -r ${VAL_RUTA}/input/*
ftp -inv $VAL_FTP_HOSTNAME $VAL_FTP_PUERTO1 <<EOF >> $VAL_LOG
user $VAL_FTP_USER $VAL_FTP_PASS
bin
pwd
cd ${VAL_FTP_RUTA}
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_NOM_ARCHIVO1_0}
mget ${VAL_NOM_ARCHIVO1_2_SIN}
mget ${VAL_NOM_ARCHIVO1_3_SIN}
mget ${VAL_NOM_ARCHIVO1_4}
bye
EOF

#VALIDA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A BIGDATA
echo "==== Valida la transferencia de los archivos desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_trnsf=`egrep 'User cannot log in|Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
if [ $error_trnsf -eq 0 ];then
	echo "==== OK - La transferencia de los archivos desde el servidor FTP se realiza con EXITO ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la transferencia de los archivos desde el servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
    exit 1
fi

#REALIZA COPIA DE LOS ARCHIVOS 100 Nueva Categoria Y ASIGNACION CANAL DE VENTAS v3 PARA ELIMINAR ESPACIOS EN EL NOMBRE DE ACUERDO A LAS VARIABLES CONFIGURADAS EN params_des
cd $VAL_RUTA/input
ls -altrh *.xlsx
ix=0
for file in *.xlsx; do
	mv "$file" "${file// /_}"
    ix=$((${ix}+1))
done
ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 2: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "2" ]; then
echo "==== Realiza la transferencia de los archivos en formato Excel desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Servidor: $VAL_FTP_HOSTNAME" >> $VAL_LOG
echo "Puerto: $VAL_FTP_PUERTO2" >> $VAL_LOG
echo "Ruta: $VAL_FTP_RUTA" >> $VAL_LOG
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
echo "==== Valida la transferencia de los archivos desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_trnsf=`egrep 'User cannot log in|Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
if [ $error_trnsf -eq 0 ];then
	echo "==== OK - La transferencia de los archivos desde el servidor FTP se realiza con EXITO ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la transferencia de los archivos desde el servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
    exit 1
fi
ETAPA=3
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 3: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "3" ]; then
echo "==== Realiza la transferencia del archivo en formato Excel desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Servidor: $VAL_FTP_HOSTNAME_MP" >> $VAL_LOG
echo "Puerto: $VAL_FTP_PUERTO_MP" >> $VAL_LOG
echo "Ruta: $VAL_FTP_RUTA_MP" >> $VAL_LOG
ftp -inv $VAL_FTP_HOSTNAME_MP $VAL_FTP_PUERTO_MP <<EOF >> $VAL_LOG
user $VAL_FTP_USER_MP $VAL_FTP_PASS_MP
bin
cd ${VAL_FTP_RUTA_MP}
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_NOM_ARCHIVO_MP}
bye
EOF

#VALIDA LA TRANSFERENCIA DEL ARCHIVO DESDE EL SERVIDOR FTP A BIGDATA
echo "==== Valida la transferencia del archivo desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_trnsf=`egrep 'User cannot log in|Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
if [ $error_trnsf -eq 0 ];then
	echo "==== OK - La transferencia del archivo desde el servidor FTP se realiza con EXITO ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la transferencia del archivo desde el servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
    exit 1
fi
ETAPA=4
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 3 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='4' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 4: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "4" ]; then
echo "==== Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
/usr/hdp/current/spark2-client/bin/spark-submit ${VAL_RUTA}/bin/read_excel_carga_hive.py --rutain=$VAL_RUTA_ARCHIVO_1_2 --tablaout=$VAL_BASE_DATOS.$VAL_TABLA_T --tipo=$VAL_TIPO_CARGA &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

#VALIDA CONTEO DE REGISTROS TABLA otc_t_catalogo_terminales
echo "==== Valida conteo de registros tabla otc_t_catalogo_terminales ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_BASE_DATOS.$VAL_TABLA_T;")
echo "Cantidad registros: $cant_reg_d" >> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se cargaron datos del archivo excel 100 Nueva Categoria.xlsx a Hive con EXITO ====" >> $VAL_LOG
			else
			echo "==== ERROR: - No se cargaron datos del archivo excel 100 Nueva Categoria.xlsx a Hive ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=5
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 4 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='5' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 5: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "5" ]; then
echo "==== Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
/usr/hdp/current/spark2-client/bin/spark-submit ${VAL_RUTA}/bin/read_excel_carga_hive.py --rutain=$VAL_RUTA_ARCHIVO_1_0 --tablaout=$VAL_BASE_DATOS.$VAL_TABLA_RDR --tipo=$VAL_TIPO_CARGA &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

#VALIDA CONTEO DE REGISTROS TABLA otc_t_catalogo_ruc_das_retail
echo "==== Valida conteo de registros tabla otc_t_catalogo_ruc_das_retail ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_BASE_DATOS.$VAL_TABLA_RDR;")
echo "Cantidad registros: $cant_reg_d" >> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se cargaron datos del archivo excel LISTADO_RUC_DAS_RETAIL.xlsx a Hive con EXITO ====" >> $VAL_LOG
			else
			echo "==== ERROR: - No se cargaron datos del archivo excel LISTADO_RUC_DAS_RETAIL.xlsx a Hive ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=6
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 5 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='6' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 6: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "6" ]; then
echo "==== Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
/usr/hdp/current/spark2-client/bin/spark-submit ${VAL_RUTA}/bin/read_excel_carga_hive.py --rutain=$VAL_RUTA_ARCHIVO_2_1 --tablaout=$VAL_BASE_DATOS.$VAL_TABLA_TC --tipo=$VAL_TIPO_CARGA &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

#VALIDA CONTEO DE REGISTROS TABLA otc_t_catalogo_tipo_canal
echo "==== Valida conteo de registros tabla otc_t_catalogo_tipo_canal ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_BASE_DATOS.$VAL_TABLA_TC;")
echo "Cantidad registros: $cant_reg_d" >> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se cargaron datos del archivo excel TIPO_CANAL.xlsx a Hive con EXITO ====" >> $VAL_LOG
			else
			echo "==== ERROR: - No se cargaron datos del archivo excel TIPO_CANAL.xlsx a Hive ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=7
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 6 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='7' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 7: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "7" ]; then
echo "==== Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
/usr/hdp/current/spark2-client/bin/spark-submit ${VAL_RUTA}/bin/read_excel_carga_hive.py --rutain=$VAL_RUTA_ARCHIVO_2_0 --tablaout=$VAL_BASE_DATOS.$VAL_TABLA_UO --tipo=$VAL_TIPO_CARGA &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

#VALIDA CONTEO DE REGISTROS TABLA otc_t_catalogo_canal_online
echo "==== Valida conteo de registros tabla otc_t_catalogo_canal_online ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_BASE_DATOS.$VAL_TABLA_UO;")
echo "Cantidad registros: $cant_reg_d" >> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se cargaron datos del archivo excel USUARIOS_CANAL_ONLINE.xlsx a Hive con EXITO ====" >> $VAL_LOG
			else
			echo "==== ERROR: - No se cargaron datos del archivo excel USUARIOS_CANAL_ONLINE.xlsx a Hive ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=8
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 7 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='8' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 8: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "8" ]; then
echo "==== Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
/usr/hdp/current/spark2-client/bin/spark-submit ${VAL_RUTA}/bin/read_excel_carga_hive.py --rutain=$VAL_RUTA_ARCHIVO_1_3 --tablaout=$VAL_BASE_DATOS.$VAL_TABLA_CANAL --tipo=$VAL_TIPO_CARGA &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

#VALIDA CONTEO DE REGISTROS TABLA otc_t_asigna_canal_ventas
echo "==== Valida conteo de registros tabla otc_t_asigna_canal_ventas ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_BASE_DATOS.$VAL_TABLA_CANAL;")
echo "Cantidad registros: $cant_reg_d" >> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se cargaron datos del archivo excel ASIGNACION CANAL DE VENTAS v3.xlsx a Hive con EXITO ====" >> $VAL_LOG
			else
			echo "==== ERROR: - No se cargaron datos del archivo excel ASIGNACION CANAL DE VENTAS v3.xlsx a Hive ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=9
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 8 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='9' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 9: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "9" ]; then
echo "==== Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
/usr/hdp/current/spark2-client/bin/spark-submit ${VAL_RUTA}/bin/read_excel_carga_hive.py --rutain=$VAL_RUTA_ARCHIVO_1_4 --tablaout=$VAL_BASE_DATOS.$VAL_TABLA_SEG --tipo=$VAL_TIPO_CARGA &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

#VALIDA CONTEO DE REGISTROS TABLA otc_t_ctl_cat_seg_sub_seg
echo "==== Valida conteo de registros tabla otc_t_ctl_cat_seg_sub_seg ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_BASE_DATOS.$VAL_TABLA_SEG;")
echo "Cantidad registros: $cant_reg_d" >> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se cargaron datos del archivo excel CTL_CAT_SEG_SUB_SEG.xlsx a Hive con EXITO ====" >> $VAL_LOG
			else
			echo "==== ERROR: - No se cargaron datos del archivo excel CTL_CAT_SEG_SUB_SEG.xlsx a Hive ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=10
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 9 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='10' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 10: HACE EL LLAMADO AL ARCHIVO SPARK QUE CARGA EL CATALOGO DE EXCEL A HIVE
if [ "$ETAPA" = "10" ]; then
echo "==== Ejecuta archivo spark read_excel_carga_hive.py que carga excel a Hive ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
/usr/hdp/current/spark2-client/bin/spark-submit ${VAL_RUTA}/bin/read_excel_carga_hive.py --rutain=$VAL_RUTA_ARCHIVO_MP --tablaout=$VAL_BASE_DATOS.$VAL_TABLA_CST --tipo=$VAL_TIPO_CARGA &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark carga_excel_a_hive.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark carga_excel_a_hive.py ====" >> $VAL_LOG
	exit 1
fi

#VALIDA CONTEO DE REGISTROS TABLA otc_t_ctl_seg_terminal
echo "==== Valida conteo de registros tabla otc_t_ctl_seg_terminal ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_BASE_DATOS.$VAL_TABLA_CST;")
echo "Cantidad registros: $cant_reg_d" >> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se cargaron datos del archivo excel Catalogo_Segmento_Terminales.xlsx a Hive con EXITO ====" >> $VAL_LOG
			else
			echo "==== ERROR: - No se cargaron datos del archivo excel Catalogo_Segmento_Terminales.xlsx a Hive ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=11
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 10 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='11' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#HACE EL LLAMADO AL HQL QUE REALIZA LOS CRUCES PARA GENERAR LA INFORMACION EN LA TABLA FINAL OTC_T_TERMINALES_SIMCARDS
if [ "$ETAPA" = "11" ]; then
echo "==== Ejecuta HQL carga_otc_t_terminales_simcards.sql ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --hiveconf hive.auto.convert.sortmerge.join=true --hiveconf hive.optimize.bucketmapjoin=true --hiveconf hive.optimize.bucketmapjoin.sortedmerge=true \
--hivevar fecha_fin=$VAL_DIA_UNO --hivevar fecha_antes_ayer=$VAL_FECHA_FORMATO_PRE --hivevar fecha_inicio=$VAL_FECHA_INI \
--hivevar dia_uno_mes_sig_frmt=$VAL_DIA_UNO_MES_SIG_FRMT --hivevar ultimo_dia_act_frmt=$VAL_FECHA_FORMATO --hivevar anio_mes=$VAL_MES \
--hivevar solo_anio=$VAL_SOLO_ANIO --hivevar solo_mes=$VAL_SOLO_MES --hivevar fecha_meses_atras=$VAL_MESES_ATRAS \
--hivevar fecha_meses_atras1=$VAL_MESES_ATRAS1 --hivevar fecha_meses_atras2=$VAL_MESES_ATRAS2 \
--hivevar dia_uno_mes_act_frmt=$VAL_FECHA_FORMATO_INI --hivevar dia_uno_mes_ant_frmt=$VAL_TS_INI \
--hivevar VAL_USUARIO4=$VAL_USUARIO4 --hivevar VAL_USUARIO_FINAL=$VAL_USUARIO_FINAL -f ${VAL_RUTA}/sql/carga_otc_t_terminales_simcards.sql 2>> $VAL_LOG

#VALIDA EJECUCION DEL HQL
echo "==== Valida ejecucion del HQL carga_otc_t_terminales_simcards.sql ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
error_crea=`egrep 'FAILED:|Error|Table not found|Table already exists|Vertex|Failed to connect|Could not open client' $VAL_LOG | wc -l`
	if [ $error_crea -eq 0 ];then
		echo "==== OK - La ejecucion del HQL carga_otc_t_terminales_simcards.sql es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
		else
		echo "==== ERROR: - En la ejecucion del HQL carga_otc_t_terminales_simcards.sql ====" >> $VAL_LOG
		exit 1
	fi
ETAPA=12
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 11 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='12' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#LEE TABLA TERMINALES SIMCARDS Y GENERA ARCHIVO TXT EN RUTA OUTPUT
if [ "$ETAPA" = "12" ]; then
rm -r ${VAL_RUTA}/output/*
echo "==== Lee tabla terminales simcards y genera archivo txt en ruta output ====" >> $VAL_LOG
beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
--outputformat=dsv --delimiterForDSV='|' --showHeader=true -e "set hive.cli.print.header=true;
SELECT a.fecha_proceso AS fecha_proceso,a.fecha_factura AS fecha_factura,a.linea_negocio AS linea_negocio,a.segmento AS segmento,a.sub_segmento AS sub_segmento,a.segmento_final AS segmento_final,a.telefono AS telefono,a.clasificacion AS clasificacion,a.tipo_documento AS tipo_documento,a.num_factura AS num_factura,a.num_factura_relacionada AS num_factura_relacionada,a.fecha_factura_relacionada AS fecha_factura_relacionada,a.oficina AS oficina,a.account_num AS account_num,a.nombre_cliente AS nombre_cliente,a.tipo_doc_cliente AS tipo_doc_cliente,a.identificacion_cliente AS identificacion_cliente,a.modelo_terminal AS modelo_terminal,a.imei AS imei,a.tipo_cargo AS tipo_cargo,a.modelo_guia_comercial AS modelo_guia_comercial,a.clasificacion_terminal AS clasificacion_terminal,a.cantidad AS cantidad,a.monto AS monto,a.num_abonado AS num_abonado,a.movimiento AS movimiento,a.id_tipo_movimiento AS id_tipo_movimiento,a.id_producto AS id_producto,a.plan_codigo AS plan_codigo,a.plan_nombre AS plan_nombre,a.tarifa_basica AS tarifa_basica,a.usuario_final AS usuario_final,a.nombre_usuario_final AS nombre_usuario_final,a.tipo_venta AS tipo_venta,a.cuotas_financiadas AS cuotas_financiadas,a.ejecutivo_perimetro AS ejecutivo_perimetro,a.jefe_perimetro AS jefe_perimetro,a.gerente_perimetro AS gerente_perimetro,a.nota_credito_masiva AS nota_credito_masiva,a.forma_pago_factura AS forma_pago_factura,a.cuota_inicial AS cuota_inicial,a.canal_comercial AS canal_comercial,a.id_canal AS id_canal,a.nom_distribuidor AS nom_distribuidor,a.ruc_distribuidor AS ruc_distribuidor,a.codigo_plaza AS codigo_plaza,a.nom_plaza AS nom_plaza,a.ciudad AS ciudad,a.provincia AS provincia,a.region AS region,a.nuevo_subcanal AS nuevo_subcanal,a.id_sub_canal AS id_sub_canal,a.tipo_movimiento_mes AS tipo_movimiento_mes,a.fecha_alta AS fecha_alta,a.antiguedad_meses AS antiguedad_meses,a.linea_negocio_homologado AS linea_negocio_homologado,a.id_hash AS id_hash
FROM (SELECT '${VAL_FECHA_FORMATO}' AS fecha_proceso,
(CASE WHEN fecha_factura IS NULL THEN '01/01/1990' ELSE 
concat_ws('/',SUBSTR(fecha_factura,9,2),SUBSTR(fecha_factura,6,2),SUBSTR(fecha_factura,1,4)) END) AS fecha_factura,
linea_negocio,segmento,sub_segmento,segmento_final,telefono,clasificacion,
(CASE WHEN tipo_documento IS NULL THEN 'N/A' ELSE tipo_documento END) AS tipo_documento,
num_factura,num_factura_relacionada,(CASE WHEN fecha_factura_relacionada IS NULL THEN '01/01/1990' ELSE
concat_ws('/',SUBSTR(fecha_factura_relacionada,9,2),SUBSTR(fecha_factura_relacionada,6,2),SUBSTR(fecha_factura_relacionada,1,4)) END) AS fecha_factura_relacionada,
oficina,account_num,nombre_cliente, 
UPPER(CASE WHEN (COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(tipo_doc_cliente,'Ã¡','a'),'Ã©','e'),'Ã­','i'),'Ã³','o'),'Ãº','u'))) IS NULL THEN 'N/A' ELSE (COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(tipo_doc_cliente,'Ã¡','a'),'Ã©','e'),'Ã­','i'),'Ã³','o'),'Ãº','u'))) END) AS tipo_doc_cliente,
identificacion_cliente,modelo_terminal,imei,tipo_cargo,modelo_guia_comercial,clasificacion_terminal,cantidad,
CAST(CAST(monto AS decimal(12,2)) AS string) AS monto,num_abonado,movimiento,(CASE WHEN id_tipo_movimiento IS NULL THEN '-1' ELSE id_tipo_movimiento END) AS id_tipo_movimiento,
(CASE WHEN id_producto IS NULL THEN '-1' ELSE id_producto END) AS id_producto,plan_codigo,
plan_nombre,(CASE WHEN tarifa_basica IS NULL THEN '' ELSE CAST(CAST(tarifa_basica AS decimal(12,2)) AS string) END) AS tarifa_basica,
usuario_final,nombre_usuario_final,tipo_venta,cuotas_financiadas,ejecutivo_perimetro,jefe_perimetro,gerente_perimetro,nota_credito_masiva,
UPPER(COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(forma_pago_factura,'Ã¡','a'),'Ã©','e'),'Ã­','i'),'Ã³','o'),'Ãº','u'))) AS forma_pago_factura,
(CASE WHEN cuota_inicial IS NULL THEN '-1' ELSE CAST(CAST(cuota_inicial AS decimal(12,2)) AS string) END) AS cuota_inicial,
canal_comercial,(CASE WHEN id_canal IS NULL THEN '-1' ELSE id_canal END) AS id_canal,
oficina_usuario AS nom_distribuidor,ruc_distribuidor,codigo_plaza,nom_plaza,ciudad,provincia,region,nuevo_subcanal,
(CASE WHEN id_sub_canal IS NULL THEN '-1' ELSE id_sub_canal END) AS id_sub_canal,tipo_movimiento_mes,
(CASE WHEN fecha_alta IS NULL THEN '01/01/1990' 
ELSE concat_ws('/',SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),9,2),SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),6,2),SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),1,4)) END) AS fecha_alta,
(CASE WHEN CAST(CAST(antiguedad_meses AS decimal(12,2)) AS string)<0 THEN '-1' 
ELSE CAST(CAST(antiguedad_meses AS decimal(12,2)) AS string) END) AS antiguedad_meses,linea_negocio_homologado,id_hash
FROM db_desarrollo2021.otc_t_terminales_simcards WHERE p_fecha_factura>=${VAL_FECHA_INI} AND p_fecha_factura<${VAL_DIA_UNO}
AND clasificacion IN ('ACCESORIOS','TERMINALES') AND tipo_cargo = 'CARGO' AND tipo_documento<>'NOTA DE CREDITO'
UNION ALL
SELECT '${VAL_FECHA_FORMATO}' AS fecha_proceso,
(CASE WHEN fecha_factura IS NULL THEN '01/01/1990' ELSE 
concat_ws('/',SUBSTR(fecha_factura,9,2),SUBSTR(fecha_factura,6,2),SUBSTR(fecha_factura,1,4)) END) AS fecha_factura,
linea_negocio,segmento,sub_segmento,segmento_final,telefono,clasificacion,
(CASE WHEN tipo_documento IS NULL THEN 'N/A' ELSE tipo_documento END) AS tipo_documento,
num_factura,num_factura_relacionada,(CASE WHEN fecha_factura_relacionada IS NULL THEN '01/01/1990' ELSE
concat_ws('/',SUBSTR(fecha_factura_relacionada,9,2),SUBSTR(fecha_factura_relacionada,6,2),SUBSTR(fecha_factura_relacionada,1,4)) END) AS fecha_factura_relacionada,
oficina,account_num,nombre_cliente,
UPPER(CASE WHEN (COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(tipo_doc_cliente,'Ã¡','a'),'Ã©','e'),'Ã­','i'),'Ã³','o'),'Ãº','u'))) IS NULL THEN 'N/A' ELSE (COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(tipo_doc_cliente,'Ã¡','a'),'Ã©','e'),'Ã­','i'),'Ã³','o'),'Ãº','u'))) END) AS tipo_doc_cliente,
identificacion_cliente,modelo_terminal,imei,tipo_cargo,modelo_guia_comercial,clasificacion_terminal,cantidad,
CAST(CAST(monto AS decimal(12,2)) AS string) AS monto,num_abonado,movimiento,(CASE WHEN id_tipo_movimiento IS NULL THEN '-1' ELSE id_tipo_movimiento END) AS id_tipo_movimiento,
(CASE WHEN id_producto IS NULL THEN '-1' ELSE id_producto END) AS id_producto,plan_codigo,
plan_nombre,(CASE WHEN tarifa_basica IS NULL THEN '' ELSE CAST(CAST(tarifa_basica AS decimal(12,2)) AS string) END) AS tarifa_basica,
usuario_final,nombre_usuario_final,tipo_venta,cuotas_financiadas,ejecutivo_perimetro,jefe_perimetro,gerente_perimetro,nota_credito_masiva,
UPPER(COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(forma_pago_factura,'Ã¡','a'),'Ã©','e'),'Ã­','i'),'Ã³','o'),'Ãº','u'))) AS forma_pago_factura,
(CASE WHEN cuota_inicial IS NULL THEN '-1' ELSE CAST(CAST(cuota_inicial AS decimal(12,2)) AS string) END) AS cuota_inicial,
canal_comercial,(CASE WHEN id_canal IS NULL THEN '-1' ELSE id_canal END) AS id_canal,
oficina_usuario AS nom_distribuidor,ruc_distribuidor,codigo_plaza,nom_plaza,ciudad,provincia,region,nuevo_subcanal,
(CASE WHEN id_sub_canal IS NULL THEN '-1' ELSE id_sub_canal END) AS id_sub_canal,tipo_movimiento_mes,
(CASE WHEN fecha_alta IS NULL THEN '01/01/1990' 
ELSE concat_ws('/',SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),9,2),SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),6,2),SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),1,4)) END) AS fecha_alta,
(CASE WHEN CAST(CAST(antiguedad_meses AS decimal(12,2)) AS string)<0 THEN '-1' 
ELSE CAST(CAST(antiguedad_meses AS decimal(12,2)) AS string) END) AS antiguedad_meses,linea_negocio_homologado,id_hash
FROM db_desarrollo2021.otc_t_terminales_simcards WHERE p_fecha_factura>=${VAL_FECHA_INI} AND p_fecha_factura<${VAL_DIA_UNO}
AND clasificacion IN ('ACCESORIOS','TERMINALES') AND tipo_cargo = 'CARGO' AND tipo_documento='NOTA DE CREDITO' 
AND concat_ws('',SUBSTR(fecha_factura_relacionada,1,4),SUBSTR(fecha_factura_relacionada,6,2),SUBSTR(fecha_factura_relacionada,9,2))>='${VAL_FECHA_INI}'
AND concat_ws('',SUBSTR(fecha_factura_relacionada,1,4),SUBSTR(fecha_factura_relacionada,6,2),SUBSTR(fecha_factura_relacionada,9,2))<'${VAL_DIA_UNO}') a;" 2>> $VAL_LOG | sed 's/NULL//g' > ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO_PREVIO

#CONVIERTE LOS NOMBRES DE LOS CAMPOS DE MINUSCULAS A MAYUSCULAS
sed -i -e '1 s/\(.*\)/\U\1/' ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO_PREVIO
#CAMBIA EL ENCODING DEL ARCHIVO PARA QUE NO GENERE CARACTERES ESPECIALES
iconv -f utf8 -t ascii//TRANSLIT ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO_PREVIO > ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO

#VERIFICA SI EL ARCHIVO TXT CONTIENE DATOS
echo "==== Valida si el archivo TXT contiene datos ====" >> $VAL_LOG
cant_reg=`wc -l ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO` 
echo $cant_reg >> $VAL_LOG
cant_reg=`echo ${cant_reg}|cut -f1 -d" "` 
cant_reg=`expr $cant_reg + 0` 
	if [ $cant_reg -ne 0 ]; then
			echo "==== OK - El archivo TXT contiene datos para transferir al servidor FTP ====" >> $VAL_LOG
		else
			echo "==== ERROR - El archivo TXT no contiene datos para transferir al servidor FTP ====" >> $VAL_LOG
			exit 1
	fi
ETAPA=13
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 12 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='13' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#CREA FUNCION PARA LA EXPORTACION DEL ARCHIVO A RUTA FTP Y REALIZA LA TRANSFERENCIA
if [ "$ETAPA" = "13" ]; then
echo "==== Crea funcion para la exportacion del archivo a ruta FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
function exportar()
{
    /usr/bin/expect << EOF >> $VAL_LOG
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

#REALIZA LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo  "==== Inicia exportacion del archivo txt al servidor SFTP ====" >> $VAL_LOG
echo "Host SFTP: $VAL_FTP_HOSTNAME_OUT" >> $VAL_LOG
echo "Puerto SFTP: $VAL_FTP_PUERTO_OUT" >> $VAL_LOG
echo "Usuario SFTP: $VAL_FTP_USER_OUT" >> $VAL_LOG
echo "Password SFTP: $VAL_FTP_PASS_OUT" >> $VAL_LOG
echo "Ruta SFTP: $VAL_FTP_RUTA_OUT" >> $VAL_LOG
exportar $VAL_NOM_ARCHIVO &>> $VAL_LOG

#VALIDA EJECUCION DE LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo "==== Valida transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
VAL_ERROR_FTP=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
	if [ $VAL_ERROR_FTP -ne 0 ]; then
		echo "==== ERROR - En la transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
		exit 1
		else
		echo "==== OK - La transferencia del archivo TXT al servidor FTP es EXITOSA ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
	fi
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 13 con EXITO ===="`date '+%H%M%S'` >> $VAL_LOG
`mysql -N  <<<"update params_des set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
	
echo "==== Finaliza ejecucion del proceso BI CS Terminales Simcards ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
