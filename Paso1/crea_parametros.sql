+---------------------+---------------------+---------------------------------------------------------------------------------------------------------------+-------+----------+
| ENTIDAD             | PARAMETRO           | VALOR                                                                                                         | ORDEN | AMBIENTE |
+---------------------+---------------------+---------------------------------------------------------------------------------------------------------------+-------+----------+
| D_SHTRMNLSMCRDS0010 | PARAM1_FECHA        | date_format(sysdate(),'%Y%m%d')                                                                               |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | PARAM2_VAL_RUTA     | '/home/nae108667/CLOUDERA/cp_terminales_simcards'                                                             |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | ETAPA               | 1                                                                                                             |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | SHELL               | /home/nae108667/CLOUDERA/cp_terminales_simcards/bin/SH_TERMINALES_SIMCARDS.sh                                 |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | PERIODICIDAD        | DIARIA                                                                                                        |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_FTP_PUERTO1     | 9559                                                                                                          |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_FTP_PUERTO2     | 9557                                                                                                          |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_FTP_USER        | ftp_contabilidad                                                                                              |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_FTP_HOSTNAME    | 10.112.47.36                                                                                                  |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_FTP_PASS        | Telefonica.2018&                                                                                              |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_FTP_RUTA        | /                                                                                                             |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_NOM_ARCHIVO1_0  | LISTADO_RUC_DAS_RETAIL.xlsx                                                                                   |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_NOM_ARCHIVO1_1  | CONCEPTOS_FACT_ADICIONALES.xlsx                                                                               |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_NOM_ARCHIVO1_2  | "100 Nueva Categoria.xlsx"                                                                                    |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_NOM_ARCHIVO2_0  | USUARIOS_CANAL_ONLINE.xlsx                                                                                    |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_NOM_ARCHIVO2_1  | TIPO_CANAL.xlsx                                                                                               |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_BASE_DATOS      | db_desarrollo2021                                                                                             |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_TABLA_TC        | OTC_T_CATALOGO_TIPO_CANAL                                                                                     |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_TABLA_RDR       | OTC_T_CATALOGO_RUC_DAS_RETAIL                                                                                 |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_TABLA_T         | OTC_T_CATALOGO_TERMINALES                                                                                     |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_TABLA_UO        | OTC_T_CATALOGO_CANAL_ONLINE                                                                                   |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_TABLA_ZTFI      | OTC_T_CONCEPTOS_FACT_ADICION                                                                                  |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_USUARIO_BACKOFF | "NAG00029","NAINJA02","NALAJA01","NAUIJA61","NAUIJA62","NAUIJA65","NAUIJA66","NAUIJA67","NAUIJA82","internal" |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_USUARIO4        | "NA002828"                                                                                                    |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_USUARIO_FINAL   | "NA400413","NA002132","NA1000206","NA002152","MFSALAZAR","NA400406","NA002420"                                |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_MESES           | 2                                                                                                             |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDUSER              | RDB_REPORTES                                                                                                  |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDPASS              | TelfEcu2017                                                                                                   |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDHOST              | proxfulldg1.otecel.com.ec                                                                                     |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDPORT              | 7594                                                                                                          |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDDB                | tomstby.otecel.com.ec                                                                                         |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDTABLE             | OTC_NC_TERM_SIMC_FINAL                                                                                        |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDUSER1             | RBM_REPORTES                                                                                                  |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDPASS1             | TelfEcu2017                                                                                                   |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDHOST1             | proxfulldg2.otecel.com.ec                                                                                     |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDPORT1             | 7594                                                                                                          |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | TDDB1               | norstby.otecel.com.ec                                                                                         |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_MASTER          | yarn                                                                                                          |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_DRIVER_MEMORY   | 32G                                                                                                           |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_EXECUTOR_MEMORY | 32G                                                                                                           |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_NUM_EXECUTORS   | 8                                                                                                             |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_CORES_EXECUTORS | 8                                                                                                             |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | VAL_TIPO_CARGA_O    | overwrite                                                                                                     |     0 |        0 |
| D_SHTRMNLSMCRDS0010 | HIVEDB_TMP          | db_desarrollo2021                                                                                             |     0 |        0 |
+---------------------+---------------------+---------------------------------------------------------------------------------------------------------------+-------+----------+

--PARAMETROS PARA LA ENTIDAD D_SHTRMNLSMCRDS0010
DELETE FROM params_des WHERE ENTIDAD='D_SHTRMNLSMCRDS0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','HIVEDB','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_MASTER','yarn','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_DRIVER_MEMORY','24G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_EXECUTOR_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_EXECUTOR_CORES','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','PARAM1_FECHA','date_format(sysdate(),''%Y%m%d'')','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_RUTA','/home/nae108834/cp_terminales_simcards','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','ETAPA','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','SHELL','/home/nae108834/cp_terminales_simcards/bin/SH_TERMINALES_SIMCARDS.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO1_0','LISTADO_RUC_DAS_RETAIL.xlsx','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO1_1','CONCEPTOS_FACT_ADICIONALES.xlsx','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO1_2','"100*Nueva*Categoria*-*2022.xlsx"','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO1_3','"ASIGNACION*CANAL*DE*VENTAS*v3.xlsx"','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO1_4','CTL_CAT_SEG_SUB_SEG.xlsx','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO2_0','USUARIOS_CANAL_ONLINE.xlsx','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_SFTP_RUTA_1','''"/Inteligencia~}<de~}<Mercado/07_Analisis_epc-rlf/02_Análisis/Terminales/Catalogos"''','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_SFTP_RUTA_2','''"/Business~}<Intelligence/3_Operaciones/Procesos/EMERGENTE/NC/PARQUE/CATALOGO"''','0','0');

INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO2_1','TIPO_CANAL.xlsx','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_NOM_ARCHIVO_MP','Catalogo_Segmento_Terminales.xlsx','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_BASE_DATOS','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_TC','otc_t_catalogo_tipo_canal','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_CANAL','otc_t_asigna_canal_ventas','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_SEG','otc_t_ctl_cat_seg_sub_seg','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_CST','otc_t_ctl_seg_terminal','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_RDR','otc_t_catalogo_ruc_das_retail','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_T','otc_t_catalogo_terminales','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_UO','otc_t_catalogo_canal_online','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TABLA_ZTFI','OTC_T_CONCEPTOS_FACT_ADICION','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_USUARIO_BACKOFF','"NAG00029","NAINJA02","NALAJA01","NAUIJA61","NAUIJA62","NAUIJA65","NAUIJA66","NAUIJA67","NAUIJA82","internal"','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_USUARIO4','"NA002828"','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_USUARIO_FINAL','"NA400413","NA002132","NA1000206","NA002152","MFSALAZAR","NA400406","NA002420"','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_MESES','2','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_MESES1','6','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_MESES2','18','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_SHTRMNLSMCRDS0010','VAL_TIPO_CARGA','overwrite','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_SHTRMNLSMCRDS0010';

--PARAMETROS PARA LA ENTIDAD D_URMCBMBILL4060
DELETE FROM params_des WHERE entidad='D_URMCBMBILL4060';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','RUTA','/home/nae108834/cp_terminales_simcards',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','SHELL','/home/nae108834/cp_terminales_simcards/bin/OTC_T_R_CBM_BILL.sh',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','TDTABLE','r_cbm_bill',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','HIVEDB','db_desarrollo2021',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','VAL_TIPO_CARGA','overwrite',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','VAL_MASTER','local','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','VAL_DRIVER_MEMORY','32G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','VAL_EXECUTOR_MEMORY','32G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','VAL_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMCBMBILL4060','VAL_NUM_EXECUTORS_CORES','4','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_URMCBMBILL4060';

--PARAMETROS PARA LA ENTIDAD D_URMTRMNLSVUSR4050
DELETE FROM params_des WHERE entidad='D_URMTRMNLSVUSR4050';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','VAL_RUTA','/home/nae108834/cp_terminales_simcards',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','SHELL','/home/nae108834/cp_terminales_simcards/bin/OTC_T_V_USUARIOS.sh',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','TDTABLE','otc_v_usuarios',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','HIVEDB','db_desarrollo2021',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','VAL_TIPO_CARGA','overwrite',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','VAL_MASTER','local','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','VAL_DRIVER_MEMORY','32G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','VAL_EXECUTOR_MEMORY','32G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','VAL_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTRMNLSVUSR4050','VAL_NUM_EXECUTORS_CORES','4','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_URMTRMNLSVUSR4050';

--PARAMETROS PARA LA ENTIDAD D_TRMNLSFCT0040
DELETE FROM params_des WHERE entidad='D_TRMNLSFCT0040';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','VAL_RUTA','/home/nae108834/cp_terminales_simcards',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','SHELL','/home/nae108834/cp_terminales_simcards/bin/OTC_T_TERMINALES_FACT.sh',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','TDTABLE','otc_t_terminales_fact',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','HIVEDB','db_desarrollo2021',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','VAL_TIPO_CARGA','overwrite',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','VAL_MASTER','local','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','VAL_DRIVER_MEMORY','32G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','VAL_EXECUTOR_MEMORY','32G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','VAL_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSFCT0040','VAL_NUM_EXECUTORS_CORES','4','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_TRMNLSFCT0040';

--PARAMETROS PARA LA ENTIDAD D_TRMNLSNC0030
DELETE FROM params_des WHERE entidad='D_TRMNLSNC0030';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','VAL_RUTA','/home/nae108834/cp_terminales_simcards',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','SHELL','/home/nae108834/cp_terminales_simcards/bin/OTC_T_TERMINALES_NC.sh',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','TDTABLE','otc_t_terminales_nc',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','HIVEDB','db_desarrollo2021',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','VAL_TIPO_CARGA','overwrite',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','VAL_MASTER','local','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','VAL_DRIVER_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','VAL_EXECUTOR_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','VAL_NUM_EXECUTORS','4','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_TRMNLSNC0030','VAL_NUM_EXECUTORS_CORES','4','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_TRMNLSNC0030';




