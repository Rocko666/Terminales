------------------------------------------------------------------------------
--###############################################################
--# Proyecto:             OTT_267846                            #
--# Cliente:              Telefonica                            #
--# Elaborado por:        INDRA BI                              #
--# Script:               Consulta_Terminales_Simcards_00.sql   #
--# Fecha de creacion:    2018/10/26                            #
--# Fecha de modificacioón:                                      #
--###############################################################
------------------------------------------------------------------------------------------------------------------------------
-- MODIFICACIONES
-- FECHA         AUTOR                          DESCRIPCION MOTIVO
-- 2021-12-16    Karina Castro Softconsulting   Se unen los sql Consulta_Terminales_Simcards_00.sql,
-- Consulta_Terminales_Simcards_01.sql y Consulta_Terminales_Simcards_02.sql. Se renombran las tablas
-- que son temporales y se cambia el esquema db_cs_terminales, por db_desarrollo2021.
-- 2022-02-14    Karina Castro Softconsulting   Se agregan logicas para algunos campos. Buscar con esta fecha de cambio. 
-------------------------------------------------------------------------------------------------------------------------------
-- MODIFICACIONES
-- FECHA         AUTOR                             DESCRIPCION MOTIVO
-- 2021-12-16    Karina Castro - Softconsulting    Se unen los sql en uno solo.
-- Se ubican las temporales en el esquema adecuado y los nombres de las mismas
-- se cambia con el prefijo. Se ajusta todo el flujo.
--------------------------------------------------------------------------------------------------------------------------------
-- MODIFICACIONES
-- FECHA         AUTOR                             DESCRIPCION MOTIVO
-- 2022-07-12    Karina Castro - Softconsulting    Se actualiza todo el HQL de acuerdo al flujo del proceso de Andres en SAS
---------------------------------------------------------------------------------------------------------------------------------
--SET VARIABLES
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;

--SE REALIZA EL BORRADO DE LAS TABLAS TEMPORALES AL INICIO
DROP TABLE IF EXISTS db_desarrollo2021.tmp_catalogo_terminales_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_facturas_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_notas_credito_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_orden_venta_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_imei_articulo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_facturacion_usuario_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_nota_credito_usuario_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_une_fact_notascred_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_clientes_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_clientes_categoria_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_imei_cod_articulo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_agrupa_imei_codart_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_resto_segmento_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_terminal_equipo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_usuario_cm_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_trmnl_eqp_canal_usu_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_trmneqp_fuente_canal_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_trmneqp_cruza_ruc_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_trmneqp_update_canal_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_trmneqp_full_name_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mov_parque_completa_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mov_parque_completa_ant_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mov_parque_sin_dup_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mov_parque_ant_sin_dup_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_movparque_sin_dup_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_movparque_seg_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_universo_ppal_mov_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mov_parque_antiguo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mov_parque_nuevo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_movimientos_bi_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_trmneqp_movimiento_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_cuentas_un_min_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mines1_cuenta_un_min_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mov_mines_bi_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_mov_pre_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_cuentas_sin_min_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_cuentas_completa_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_ppal_completa_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_cuenta_segmento_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_cuenta_seg_masivo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_universo_fact_mov_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_universo_fact_mov2_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_perimetros_unicos_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_mov_perimetro_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_mov_update_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_mines_planes_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_mov_final_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_mov_final_upd_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_mov_final_upd1_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_mov_final_costo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_x_modelo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_sin_imei_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_rep_anterior_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fac_final_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_cuota_monto_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_cuotas_financiadas_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_concepto_articulo_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_billsummary_billseq_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_tipcanal_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_campos_para_nc_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costos_fact_final_v2_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_update1_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_update2_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_update3_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_update4_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_update5_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_final_update6_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v2_1_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v3_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v4_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v4up_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v5up_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v5_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_tarjeta_banco_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_tarjeta_banco1_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_pivot_tarjeta_banco_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v6_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_final_v4_1_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_exporta_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_exporta_otra_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_costo_fact_exporta_otra1_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_fact_exporta_nodupli_csts;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_terminales_simcards_nc;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_terminales_simcards_factura;
DROP TABLE IF EXISTS db_desarrollo2021.tmp_terminales_simcards;

--CREA TEMPORAL CON ALGUNAS TRANSFORMACIONES A CAMPOS DEL CATALOGO DE TERMINALES
CREATE TABLE db_desarrollo2021.tmp_catalogo_terminales_csts AS
SELECT a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion_terminal,
a.concepto_facturable,
a.clasificacion
FROM (SELECT SEGMENTACION_SMARTS
,(CASE WHEN FABRICANTE='nan' THEN NULL ELSE FABRICANTE END) AS FABRICANTE
,MODELO_GUIA_COMERCIAL
,(CASE WHEN descripcion_gama='nan' THEN NULL ELSE descripcion_gama END) AS descripcion_gama
,(CASE WHEN `2g_/_3g`='nan' THEN NULL ELSE `2g_/_3g` END) AS 2G_3G
,UPPER(MODELO_SCL) AS MODELO_SCL
,CASE WHEN UPPER(descripcion_gama) IN ('PREMIUN','PREMIUM') THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'SIM' THEN 'CHIP'
    WHEN UPPER(descripcion_gama) = 'MEDIA' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'T-DIGITAL' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'MODEM' THEN 'MODEM'
    WHEN UPPER(descripcion_gama) = 'TABLETS' THEN 'TABLETS'
    WHEN UPPER(descripcion_gama) = 'BAJA' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'ALTA' THEN 'TELEFONO' 
    WHEN UPPER(descripcion_gama) = 'BASES FIJAS' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'NETBOOK' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'LOCALIZADOR' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'ROUTER' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) = 'BASE FIJA' THEN 'TELEFONO'
    WHEN UPPER(descripcion_gama) IN ('BASIC','MEDIA BAJA') THEN 'TELEFONO'
 ELSE 'ND'
 END CLASIFICACION_TERMINAL,
 CONCEPTO_FACTURABLE,
 clasificacion,
row_number() OVER (PARTITION BY concepto_facturable ORDER BY modelo_scl DESC) AS rn
FROM db_desarrollo2021.otc_t_catalogo_terminales)a 
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL DE LA FACTURACION
CREATE TABLE db_desarrollo2021.tmp_facturas_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.invoice_num,
a.office_code,
a.office_name,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
b.segmentacion_smarts,
b.fabricante,
b.modelo_guia_comercial,
b.descripcion_gama,
b.2g_3g,
b.modelo_scl,
b.clasificacion_terminal,
b.concepto_facturable,
b.clasificacion
FROM db_desarrollo2021.otc_t_terminales_fact a
INNER JOIN db_desarrollo2021.tmp_catalogo_terminales_csts b
ON a.revenue_code_id=b.concepto_facturable
WHERE a.pt_fecha>=${fecha_inicio} AND a.pt_fecha<${fecha_fin};

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL DE NOTAS CREDITO
CREATE TABLE db_desarrollo2021.tmp_notas_credito_csts AS
SELECT a.office_code,
a.office_name,
a.usuario,
a.account_num,
a.num_abonado,
a.bill_dtm,
a.document_type_id,
a.documento,
a.origin_doc_type_id,
'NOTA DE CREDITO'  AS document_type_name,
a.nro_nota_credito,
a.origin_invoice_num,
a.customer_id_type,
a.customer_id_number,
a.nombre_cliente,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.monto,
b.segmentacion_smarts,
b.fabricante,
b.modelo_guia_comercial,
b.descripcion_gama,
b.2g_3g,
b.modelo_scl,
b.clasificacion_terminal,
b.concepto_facturable,
b.clasificacion
FROM db_desarrollo2021.otc_t_terminales_nc a
INNER JOIN db_desarrollo2021.tmp_catalogo_terminales_csts b
ON a.revenue_code_id=b.concepto_facturable
WHERE a.pt_fecha>=${fecha_inicio} AND a.pt_fecha<${fecha_fin};

--CREA TABLA TEMPORAL CON LA INFORMACION DE ORDEN DE VENTA PARA LOS type_id = 9062352550013045460
CREATE TABLE db_desarrollo2021.tmp_orden_venta_csts AS
SELECT a.name, 
a.bill_sequence_number,
a.sales_order_id,
b.name AS orden_de_venta,
b.created_by,
b.created_when,
b.modified_by,
b.modified_when,
b.processed_when,
b.sales_order_owner,
(CASE WHEN b.sales_order_owner=110 THEN 'internal' ELSE c.domain_login END) AS domain_login_ow,
c.domain_name AS domain_name_ow,
c.full_name AS full_name_ow,
b.submitted_by,
b.submitted_when,
(CASE WHEN b.submitted_by=110 THEN 'internal' ELSE d.domain_login END) AS domain_login_sub,
d.domain_name AS domain_name_sub,
d.full_name AS full_name_sub,
x.name as usuario_factura,
x.full_name as nombre_usuario_factura,
x.name as codigo_creador_orden_venta,
x.full_name as nombre_creador_orden_venta,
(CASE WHEN b.sales_order_owner=110 THEN 'internal' ELSE c.name END) AS codigo_propietario_orden_venta,
c.full_name AS nombre_propietario_orden_venta,
(CASE WHEN b.submitted_by=110 THEN 'internal' ELSE d.name END) AS codigo_confirmador_orden_venta,
d.full_name AS nombre_confirmador_orden_venta
FROM db_desarrollo2021.otc_t_r_cbm_bill a --db_rdb
INNER JOIN db_rdb.otc_t_r_boe_sales_ord b 
ON (a.sales_order_id=b.object_id 
AND b.type_id = 9062352550013045460)
LEFT JOIN db_rdb.otc_t_r_usr_users x 
ON b.created_by=x.name
LEFT JOIN db_rdb.otc_t_r_usr_users c 
ON b.sales_order_owner=c.object_id
LEFT JOIN db_rdb.otc_t_r_usr_users d 
ON (b.submitted_by=d.object_id)
WHERE (b.pt_created_when >= ${fecha_meses_atras} AND b.pt_created_when < ${fecha_fin});

--CREA TABLA TEMPORAL CON LA INFORMACION DEL ARTICULO PARA CADA IMEI
CREATE TABLE db_desarrollo2021.tmp_imei_articulo_csts AS
SELECT cpe.imei imei,
cpe.created_when,      
cpe_mod.article AS codigo_articulo,
cpe_mod.name AS nombre_articulo,
cpe.purchase_price,
cpe.modified_when   
FROM db_desarrollo2021.otc_t_r_am_cpe cpe --db_rdb
INNER JOIN db_rdb.otc_t_r_am_cpe_model cpe_mod 
ON cpe_mod.object_id = cpe.stock_item_model;

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA FACTURACION CRUZADA CON USUARIO
CREATE TABLE db_desarrollo2021.tmp_facturacion_usuario_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.invoice_num,
a.office_code,
a.office_name,
a.usuario,
b.usuario AS domain_login,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion_terminal,
a.concepto_facturable,
a.clasificacion
FROM db_desarrollo2021.tmp_facturas_csts a
LEFT JOIN db_desarrollo2021.otc_t_v_usuarios b
ON a.usuario = b.object_id_user;

--CREA TABLA TEMPORAL CON LA INFORMACION DE NOTA CREDITO CRUZADA CON USUARIO
CREATE TABLE db_desarrollo2021.tmp_nota_credito_usuario_csts AS
SELECT a.office_code,
a.office_name,
a.usuario,
b.usuario AS domain_login,
a.account_num,
a.num_abonado,
a.bill_dtm,
a.document_type_id,
a.documento,
a.origin_doc_type_id,
a.document_type_name,
a.nro_nota_credito,
a.origin_invoice_num,
a.customer_id_type,
a.customer_id_number,
a.nombre_cliente,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.monto,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion_terminal,
a.concepto_facturable,
a.clasificacion
FROM db_desarrollo2021.tmp_notas_credito_csts a
LEFT JOIN db_desarrollo2021.otc_t_v_usuarios b
ON a.usuario = b.object_id_user;

--CREA TABLA TEMPORAL CON LA UNION DE FACTURAS Y NOTAS CREDITO
CREATE TABLE db_desarrollo2021.tmp_une_fact_notascred_csts AS
SELECT fecha_factura AS fecha,
bill_status,
sri_authorization_date,
document_type_id,
document_type_name,
NULL AS origin_doc_type_id,
'' AS documento,
invoice_num AS num_fact_nc,
'' AS origin_invoice_num,
office_code,
office_name,
usuario,
domain_login,
account_num,
num_abonado,
nombre_cliente,
'' AS customer_id_type,
customer_id_number,
revenue_code_id,
revenue_code_desc,
imei_imsi,
product_quantity,
monto,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion_terminal,
concepto_facturable,
clasificacion
FROM db_desarrollo2021.tmp_facturacion_usuario_csts
UNION ALL
SELECT bill_dtm AS fecha,
NULL AS bill_status,
NULL AS sri_authorization_date,
document_type_id,
document_type_name,
origin_doc_type_id,
documento,
nro_nota_credito AS num_fact_nc,
origin_invoice_num,
office_code,
office_name,
usuario,
domain_login,
account_num,
num_abonado,
nombre_cliente,
customer_id_type,
customer_id_number,
revenue_code_id,
revenue_code_desc,
imei_imsi,
-1 AS product_quantity,
monto,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion_terminal,
concepto_facturable,
clasificacion
FROM db_desarrollo2021.tmp_nota_credito_usuario_csts;

--CREA TABLA TEMPORAL CON LA INFORMACION DE CLIENTES
CREATE TABLE db_desarrollo2021.tmp_clientes_csts AS
SELECT a.account_num,
a.segmento,
a.tipo_doc_cliente,
a.num_ident,
a.nom_cliente,
a.forma_pago_factura,
a.num_telefonico,
a.tipo_movimiento_mes,
a.fecha_alta,
b.cod_categoria,
b.des_categoria,
b.subsegmento_up,
a.linea_negocio_homologado
FROM (SELECT account_num,
segmento,
tipo_doc_cliente,
identificacion_cliente AS num_ident,
nombre_cliente AS nom_cliente,
forma_pago_factura,
sub_segmento,
num_telefonico,
tipo_movimiento_mes,
fecha_alta,
linea_negocio_homologado,
row_number() OVER (PARTITION BY account_num ORDER BY fecha_alta DESC) AS rn
FROM db_reportes.otc_t_360_general
WHERE fecha_proceso=${fecha_antes_ayer}) a
LEFT JOIN db_desarrollo2021.otc_t_ctl_cat_seg_sub_seg b
ON UPPER(a.sub_segmento)=UPPER(b.subsegmento_up)
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE CLIENTES POR CATEGORIA
CREATE TABLE db_desarrollo2021.tmp_clientes_categoria_csts AS
SELECT a.fecha,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.origin_doc_type_id,
a.documento,
a.num_fact_nc,
a.origin_invoice_num,
a.office_code,
a.office_name,
a.usuario,
a.domain_login,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion_terminal,
a.concepto_facturable,
a.clasificacion,
b.cod_categoria,
b.num_telefonico,
b.forma_pago_factura,
b.tipo_movimiento_mes,
b.fecha_alta
FROM db_desarrollo2021.tmp_une_fact_notascred_csts a
LEFT JOIN db_desarrollo2021.tmp_clientes_csts b
ON a.account_num=b.account_num;

--CREA TABLA TEMPORAL CON LA INFORMACION DE IMEI CON CODIGO ARTICULO
CREATE TABLE db_desarrollo2021.tmp_imei_cod_articulo_csts AS
SELECT a.imei_imsi, 
a.revenue_code_id, 
a.revenue_code_desc, 
b.codigo_articulo, 
b.nombre_articulo, 
b.created_when
FROM db_desarrollo2021.tmp_clientes_categoria_csts a
INNER JOIN db_desarrollo2021.tmp_imei_articulo_csts b
ON a.imei_imsi=b.imei;

--CREA TABLA TEMPORAL CON LA INFORMACION AGRUPADA DE IMEI CON CODIGO ARTICULO
CREATE TABLE db_desarrollo2021.tmp_agrupa_imei_codart_csts AS
SELECT DISTINCT a.revenue_code_id, 
a.revenue_code_desc, 
a.codigo_articulo, 
a.nombre_articulo
FROM (SELECT revenue_code_id, 
revenue_code_desc, 
codigo_articulo, 
nombre_articulo
FROM db_desarrollo2021.tmp_imei_cod_articulo_csts
GROUP BY revenue_code_id, 
revenue_code_desc, 
codigo_articulo, 
nombre_articulo) a;

--CREA TABLA TEMPORAL CON LA INFORMACION PARA EL RESTO DE SEGMENTOS
CREATE TABLE db_desarrollo2021.tmp_resto_segmento_csts AS
SELECT a.fecha,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.origin_doc_type_id,
a.documento,
a.num_fact_nc,
a.origin_invoice_num,
a.office_code,
a.office_name,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
a.domain_login,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion_terminal,
a.concepto_facturable,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
(CASE WHEN c.segmento='SIN SEGMENTO' AND c.segmentacion<>'SIN SEGMENTO' THEN 'INDIVIDUAL' ELSE c.segmento END) AS segmento,
c.segmentacion AS sub_segmento,
d.codigo_articulo,
COALESCE(d.nombre_articulo,a.revenue_code_desc) AS nombre_articulo
FROM db_desarrollo2021.tmp_clientes_categoria_csts a
LEFT JOIN db_rbm.otc_t_creditclass b
ON a.cod_categoria=b.credit_class_id
LEFT JOIN (SELECT DISTINCT segmento, UPPER(segmentacion) AS segmentacion FROM db_cs_altas.otc_t_homologacion_segmentos) c
ON UPPER(b.credit_class_name)=c.segmentacion
LEFT JOIN db_desarrollo2021.tmp_agrupa_imei_codart_csts d
ON a.revenue_code_id=d.revenue_code_id;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON EL CATALOGO DE TERMINALES
CREATE TABLE db_desarrollo2021.tmp_terminal_equipo_csts AS
SELECT a.fecha,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.origin_doc_type_id,
a.documento,
a.num_fact_nc,
a.origin_invoice_num,
a.office_code,
a.office_name,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.concepto_facturable,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.segmento,
a.sub_segmento,
a.codigo_articulo,
a.nombre_articulo,
(CASE WHEN (a.revenue_code_id IN ('9572','9573','9545','9581','9550')) THEN 'ANTICIPOS DE EQUIPO'
WHEN (a.revenue_code_id IN ('2907','2908','3956','9242','9243','9246','18355')) THEN 'ADENDUM'
WHEN a.revenue_code_id NOT IN ('9572','9573','9545','9581','9550','2907','2908','3956','9242','9243','9246') 
AND a.revenue_code_desc LIKE '%DESCUENTO%' THEN 'DESCUENTO' ELSE 'CARGO' END) AS tipo_cargo,
COALESCE(b.segmentacion_smarts,c.segmentacion_smarts) AS segmentacion_smarts, 
COALESCE(b.fabricante,c.fabricante) AS fabricante, 
COALESCE(b.modelo_guia_comercial,c.modelo_guia_comercial) AS modelo_guia_comercial,            
COALESCE(b.descripcion_gama,c.descripcion_gama) AS gama_equipo, 
(CASE WHEN TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama)) IN ('PREMIUN','PREMIUM') THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='SIM' THEN 'CHIP'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='MEDIA' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='MEDIA BAJA' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='T-DIGITAL' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='MODEM' THEN 'MODEM'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='TABLETS' THEN 'TABLETS'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='BAJA' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='ALTA' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='BASES FIJAS' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='BASE FIJA' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='NETBOOK' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='LOCALIZADOR' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='ROUTER' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='PREMIUN' THEN 'TELEFONO'
WHEN  TRIM(COALESCE(b.descripcion_gama,c.descripcion_gama))='BASE FIJA' THEN 'TELEFONO'
ELSE 'ND' END) AS clasificacion_terminal,
'2G / 3G' AS tecnologia
FROM db_desarrollo2021.tmp_resto_segmento_csts a
LEFT JOIN db_desarrollo2021.tmp_catalogo_terminales_csts b
ON a.concepto_facturable=b.concepto_facturable
AND b.fabricante=''
LEFT JOIN db_desarrollo2021.tmp_catalogo_terminales_csts c
ON a.concepto_facturable=c.concepto_facturable;

--CREA TABLA TEMPORAL CON LA INFORMACION DE USUARIOS SIN DUPLICADOS ANTES DEL PRIMER DIA DEL SIGUIENTE MES
CREATE TABLE db_desarrollo2021.tmp_usuario_cm_csts AS
SELECT a.fecha,
a.usuario,
a.nom_usuario,
a.canal,
a.campania,
a.oficina,
a.codigo_distribuidor,
a.nom_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.sub_canal,
a.ruc_distribuidor,
a.fechacarga,
a.region_global,
a.observacion,
a.nuevo_subcanal
FROM (SELECT fecha,
usuario,
nom_usuario,
canal,
campania,
oficina,
codigo_distribuidor,
nom_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
sub_canal,
ruc_distribuidor,
fechacarga,
region_global,
observacion,
nuevo_subcanal,
row_number() OVER (PARTITION BY usuario ORDER BY fecha DESC) AS rn
FROM db_cs_altas.otc_t_ctl_pos_usr_nc
WHERE fecha<'${dia_uno_mes_sig_frmt}') a
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE ORDEN DE VENTA CON NUEVO CAMPO DE CRUCE
CREATE TABLE db_desarrollo2021.tmp_trmnl_eqp_canal_usu_csts AS
SELECT x.fecha,
x.bill_status,
x.sri_authorization_date,
x.document_type_id,
x.document_type_name,
x.origin_doc_type_id,
x.documento,
x.num_fact_nc,
x.origin_invoice_num,
x.office_code,
x.office_name,
x.usuario,
x.account_num,
x.num_abonado,
x.nombre_cliente,
x.customer_id_type,
x.customer_id_number,
x.revenue_code_id,
x.revenue_code_desc,
x.imei_imsi,
x.product_quantity,
x.monto,
x.domain_login,
x.descripcion_gama,
x.2g_3g,
x.modelo_scl,
x.concepto_facturable,
x.clasificacion,
x.cod_categoria,
x.forma_pago_factura,
x.tipo_movimiento_mes,
x.fecha_alta,
x.num_telefonico,
x.segmento,
x.sub_segmento,
x.codigo_articulo,
x.nombre_articulo,
x.tipo_cargo,
x.segmentacion_smarts,
x.fabricante,
x.modelo_guia_comercial,
x.gama_equipo,
x.clasificacion_terminal,
x.tecnologia,
x.band,
c.nom_distribuidor AS oficina_usuario,
c.ruc_distribuidor,
c.codigo_plaza,
c.nom_plaza,
c.ciudad,
c.provincia,
c.region,
c.sub_canal AS nuevo_subcanal,
x.domain_login_ow,
x.domain_name_ow,
x.domain_login_sub,
x.domain_name_sub,
x.orden_de_venta,
x.created_by,
x.created_when,
x.usuario_factura,
x.nombre_usuario_factura,
x.codigo_creador_orden_venta,
x.nombre_creador_orden_venta,
x.codigo_propietario_orden_venta,
x.nombre_propietario_orden_venta,
x.codigo_confirmador_orden_venta,
x.nombre_confirmador_orden_venta,
x.usuario_cruzar,
x.cruzar_por,
x.usuario_b AS usuario_final_nuevo,
(CASE WHEN x.domain_login_ow='internal' AND x.domain_login_sub='internal'
AND x.document_type_id<>25 AND x.fecha>='2018-01-01 00:00:00' 
THEN 'CCC- CANAL ON LINE' ELSE c.canal END) AS canal
FROM(SELECT a.fecha,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.origin_doc_type_id,
a.documento,
a.num_fact_nc,
a.origin_invoice_num,
a.office_code,
a.office_name,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.concepto_facturable,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.segmento,
a.sub_segmento,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
(CASE WHEN b.domain_login_ow IS NOT NULL AND b.domain_login_sub IS NOT NULL 
AND b.domain_login_ow<>'internal' AND b.domain_login_sub<>'internal' 
AND (CASE WHEN b.domain_login_ow='internal' THEN b.domain_login_sub ELSE b.created_by END)<>a.usuario
THEN 1 ELSE 0 END) AS band,
b.domain_login_ow,
b.domain_name_ow,
b.domain_login_sub,
b.domain_name_sub,
b.orden_de_venta,
b.created_by,
b.created_when,
b.usuario_factura,
b.nombre_usuario_factura,
b.codigo_creador_orden_venta,
b.nombre_creador_orden_venta,
b.codigo_propietario_orden_venta,
b.nombre_propietario_orden_venta,
b.codigo_confirmador_orden_venta,
b.nombre_confirmador_orden_venta,
(CASE WHEN b.domain_login_ow<>'internal' AND b.domain_login_ow IS NOT NULL THEN b.domain_login_ow
WHEN (b.domain_login_ow='internal' OR b.domain_login_ow IS NULL) AND (b.domain_login_sub IS NOT NULL AND b.domain_login_sub<>'internal') THEN b.domain_login_sub
WHEN ((b.domain_login_ow='internal' OR b.domain_login_ow IS NULL) AND (b.domain_login_sub='internal' OR b.domain_login_sub IS NULL)) 
AND (b.created_by<>'internal' AND b.created_by IS NOT NULL) 
THEN b.created_by ELSE a.domain_login END) AS usuario_cruzar,
(CASE WHEN b.domain_login_ow<>'internal' AND b.domain_login_ow IS NOT NULL THEN 'DOMAIN_LOGIN_OW'
WHEN (b.domain_login_ow='internal' OR b.domain_login_ow IS NULL) AND (b.domain_login_sub IS NOT NULL AND b.domain_login_sub<>'internal') THEN 'DOMAIN_LOGIN_SUB'
WHEN ((b.domain_login_ow='internal' OR b.domain_login_ow IS NULL) AND (b.domain_login_sub='internal' OR b.domain_login_sub IS NULL)) 
AND (b.created_by<>'internal' AND b.created_by IS NOT NULL) 
THEN 'CREATED_BY' ELSE 'USUARIO FACT' END) AS cruzar_por,
(CASE WHEN b.domain_login_ow<>'internal' AND b.domain_login_ow IS NOT NULL THEN b.domain_login_ow
WHEN (b.domain_login_ow='internal' OR b.domain_login_ow IS NULL) 
AND (b.domain_login_sub IS NOT NULL AND b.domain_login_sub<>'internal') THEN b.domain_login_sub
WHEN b.domain_login_ow='internal' AND b.domain_login_sub='internal' AND b.created_by<>'internal' THEN b.created_by
ELSE a.domain_login END) AS usuario_b
FROM db_desarrollo2021.tmp_terminal_equipo_csts a
LEFT JOIN (SELECT domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
SUBSTR(name,1,INSTR(name,'.')-1) AS llave_cruce 
FROM db_desarrollo2021.tmp_orden_venta_csts) b
ON (CASE WHEN a.document_type_name='NOTA DE CREDITO' THEN a.origin_invoice_num ELSE a.num_fact_nc END)=b.llave_cruce) x
LEFT JOIN db_desarrollo2021.tmp_usuario_cm_csts c
ON x.usuario_b=c.usuario
AND c.usuario<>'internal';

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL ANTERIOR CON LA FUENTE DEL CANAL, BRANCH Y TIENDA
CREATE TABLE db_desarrollo2021.tmp_trmneqp_fuente_canal_csts AS
SELECT a.fecha,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.origin_doc_type_id,
a.documento,
a.num_fact_nc,
a.origin_invoice_num,
a.office_code,
a.office_name,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.concepto_facturable,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.segmento,
a.sub_segmento,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canal,
a.fuente_canal,
b.dist_usuario AS canl_nc,
b.tienda,
b.branch
FROM (SELECT fecha,
bill_status,
sri_authorization_date,
document_type_id,
document_type_name,
origin_doc_type_id,
documento,
num_fact_nc,
origin_invoice_num,
office_code,
office_name,
usuario,
account_num,
num_abonado,
nombre_cliente,
customer_id_type,
customer_id_number,
revenue_code_id,
revenue_code_desc,
imei_imsi,
product_quantity,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
concepto_facturable,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
segmento,
sub_segmento,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canal,
(CASE WHEN domain_login_ow IS NOT NULL AND domain_login_sub IS NOT NULL AND	domain_login_ow='internal' 
AND domain_login_sub='internal' AND document_type_id<>25 AND fecha>='2018-01-01 00:00:00' 
THEN 'OWNER Y SUBMIT INTERNAL'
WHEN (domain_login_ow IS NOT NULL AND domain_login_ow<>'internal' AND canal IS NOT NULL) THEN 'DOMAIN_LOGIN_OW'
WHEN (domain_login_sub IS NOT NULL AND (domain_login_ow='internal' OR domain_login_ow IS NULL) 
AND domain_login_sub<>'internal' AND canal IS NOT NULL) THEN 'DOMAIN_LOGIN_SUB'					
WHEN (domain_login_ow IS NULL AND domain_login_sub IS NULL AND usuario IS NOT NULL AND canal IS NOT NULL) 
THEN 'USUARIO_FACTURA' ELSE '' END) AS fuente_canal,
COALESCE(usuario_cruzar,usuario) AS test
FROM db_desarrollo2021.tmp_trmnl_eqp_canal_usu_csts) a
LEFT JOIN db_desarrollo2021.otc_t_v_usuarios b
ON a.test=b.usuario;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON EL RUC
CREATE TABLE db_desarrollo2021.tmp_trmneqp_cruza_ruc_csts AS
SELECT a.fecha,
a.bill_status,
a.sri_authorization_date,
a.document_type_id,
a.document_type_name,
a.origin_doc_type_id,
a.documento,
a.num_fact_nc,
a.origin_invoice_num,
a.office_code,
a.office_name,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.customer_id_number,
a.revenue_code_id,
a.revenue_code_desc,
a.imei_imsi,
a.product_quantity,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.concepto_facturable,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.segmento,
a.sub_segmento,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canal,
a.canl_nc,
a.tienda,
a.branch,
(CASE WHEN b.tipo_local='RETAIL' THEN 'RETAIL'
WHEN b.tipo_local='DAS' THEN 'DISTRIBUIDOR'
ELSE (COALESCE(a.canal,(CASE WHEN a.canal IS NULL AND TRIM(UPPER(a.office_name)) LIKE 'CAVS%' 
AND TRIM(UPPER(a.office_name)) NOT LIKE 'CAVS%AE%' THEN 'CAV PROPIO' ELSE a.canal END))) END) AS canal_nuevo,
b.codigo_da,
(CASE WHEN b.codigo_da IS NOT NULL THEN 'LISTADO_DAS_RETAIL' ELSE COALESCE(a.fuente_canal,'OFICINA FACTURA') END) AS fuente_canal,
UPPER(COALESCE(a.region,b.zona)) AS region,
b.razon_social,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.nuevo_subcanal
FROM db_desarrollo2021.tmp_trmneqp_fuente_canal_csts a
LEFT JOIN db_desarrollo2021.otc_t_catalogo_ruc_das_retail b
ON a.customer_id_number=b.ruc
AND a.usuario IN ('NA1008122','NA400899','NA002860','NA002569','xaherrera','NA400757','NA400415','NA184482','NA184482','NA400042'); 

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON EL RUC
CREATE TABLE db_desarrollo2021.tmp_trmneqp_update_canal_csts AS
SELECT fecha,
bill_status,
sri_authorization_date,
document_type_id,
document_type_name,
origin_doc_type_id,
documento,
num_fact_nc,
origin_invoice_num,
office_code,
office_name,
usuario,
account_num,
num_abonado,
nombre_cliente,
customer_id_type,
customer_id_number,
revenue_code_id,
revenue_code_desc,
imei_imsi,
product_quantity,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
concepto_facturable,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
segmento,
sub_segmento,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canal,
canl_nc,
tienda,
branch,
(CASE WHEN ((CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'OTROS' ELSE canal_nuevo END) IS NULL OR
TRIM(CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'OTROS' ELSE canal_nuevo END)='NO DETERMINADO' OR
TRIM(CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'OTROS' ELSE canal_nuevo END)='OTROS') AND
(TRIM(tienda) LIKE 'CAVS%' AND TRIM(tienda) NOT LIKE 'CAVS%AE%') 
THEN 'CAV PROPIO' ELSE (CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'OTROS' ELSE canal_nuevo END) END) AS canal_nuevo,
codigo_da,
(CASE WHEN ((CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'OTROS' ELSE canal_nuevo END) IS NULL OR
TRIM(CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'OTROS' ELSE canal_nuevo END)='NO DETERMINADO' OR
TRIM(CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'OTROS' ELSE canal_nuevo END)='OTROS') AND
(TRIM(tienda) LIKE 'CAVS%' AND TRIM(tienda) NOT LIKE 'CAVS%AE%') 
THEN 'NOMBRE TIENDA' ELSE (CASE WHEN canal_nuevo IS NULL OR TRIM(canal_nuevo)='NO DETERMINADO' THEN 'NO DEFINIDO' ELSE fuente_canal END) END) AS fuente_canal,
region,
razon_social,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
nuevo_subcanal
FROM db_desarrollo2021.tmp_trmneqp_cruza_ruc_csts;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON EL RUC
CREATE TABLE db_desarrollo2021.tmp_trmneqp_full_name_csts AS
SELECT a.fecha AS fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.document_type_id AS codigo_tipo_documento,
a.document_type_name AS tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_fact_nc AS num_factura,
a.origin_invoice_num,
a.office_code,
a.office_name AS oficina,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.customer_id_number AS identificacion_cliente,
a.revenue_code_id AS concepto_facturable,
a.revenue_code_desc AS modelo_terminal,
a.imei_imsi AS imei,
a.product_quantity AS cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.segmento,
a.sub_segmento,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
(CASE WHEN (a.canal_nuevo IS NULL OR TRIM(a.canal_nuevo)='NO DETERMINADO' OR TRIM(a.canal_nuevo)='OTROS')
AND (TRIM(a.tienda) NOT LIKE 'CAVS%' AND (TRIM(a.tienda) LIKE 'CAVS%AE%' OR TRIM(a.tienda) LIKE 'AE %'))
THEN 'FRANQUICIA' ELSE a.canal_nuevo END) AS canal,
a.codigo_da,
(CASE WHEN (a.canal_nuevo IS NULL OR TRIM(a.canal_nuevo)='NO DETERMINADO' OR TRIM(a.canal_nuevo)='OTROS')
AND (TRIM(a.tienda) NOT LIKE 'CAVS%' AND (TRIM(a.tienda) LIKE 'CAVS%AE%' OR TRIM(a.tienda) LIKE 'AE %'))
THEN 'NOMBRE TIENDA' ELSE a.fuente_canal END) AS fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
b.full_name AS nombre_usuario_cruzar
FROM db_desarrollo2021.tmp_trmneqp_update_canal_csts a
LEFT JOIN db_rdb.otc_t_r_usr_users b
ON UPPER(a.usuario_cruzar)=UPPER(b.name);

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA MOVIPARQUE COMPLETA PARQUE ACTUAL
CREATE TABLE db_desarrollo2021.tmp_mov_parque_completa_csts AS
SELECT num_telefonico,	
account_num,
linea_negocio,
segmento_nc AS segmento,
sub_segmento,
numero_abonado,
PLAN_CODIGO,
PLAN_NOMBRE,
fecha_alta,
fecha_baja,
tipo_doc_cliente,
documento_cliente
FROM db_cs_altas.otc_t_nc_movi_parque_v1 
WHERE fecha_proceso='${fecha_fin}';

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA MOVIPARQUE COMPLETA PARQUE ANTIGUO
CREATE TABLE db_desarrollo2021.tmp_mov_parque_completa_ant_csts AS
SELECT num_telefonico,	
account_num,
linea_negocio,
segmento_nc AS segmento,
sub_segmento,
numero_abonado,
PLAN_CODIGO,
PLAN_NOMBRE,
fecha_alta,
fecha_baja
FROM db_cs_altas.otc_t_nc_movi_parque_v1 
WHERE fecha_proceso='${fecha_inicio}';

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA MOVIPARQUE SIN DUPLICADOS
CREATE TABLE db_desarrollo2021.tmp_mov_parque_sin_dup_csts AS
SELECT a.num_telefonico,
a.account_num,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.numero_abonado,
a.PLAN_CODIGO,
a.PLAN_NOMBRE,
a.fecha_alta,
a.fecha_baja
FROM (SELECT num_telefonico,	
account_num,
linea_negocio,
segmento,
sub_segmento,
numero_abonado,
PLAN_CODIGO,
PLAN_NOMBRE,
fecha_alta,
fecha_baja,
ROW_NUMBER() over (PARTITION BY num_telefonico ORDER BY COALESCE(fecha_baja,current_timestamp()) DESC, fecha_alta DESC) rn
FROM db_desarrollo2021.tmp_mov_parque_completa_csts) a 
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA MOVIPARQUE SIN DUPLICADOS PARQUE ANTIGUO
CREATE TABLE db_desarrollo2021.tmp_mov_parque_ant_sin_dup_csts AS
SELECT a.num_telefonico,
a.account_num,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.numero_abonado,
a.PLAN_CODIGO,
a.PLAN_NOMBRE,
a.fecha_alta,
a.fecha_baja
FROM (SELECT num_telefonico,	
account_num,
linea_negocio,
segmento,
sub_segmento,
numero_abonado,
PLAN_CODIGO,
PLAN_NOMBRE,
fecha_alta,
fecha_baja,
ROW_NUMBER() over (PARTITION BY num_telefonico ORDER BY COALESCE(fecha_baja,current_timestamp()) DESC, fecha_alta DESC) rn
FROM db_desarrollo2021.tmp_mov_parque_completa_ant_csts) a 
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA MOVIPARQUE SIN DUPLICADOS
CREATE TABLE db_desarrollo2021.tmp_movparque_sin_dup_csts AS
SELECT a.account_num,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.num_telefonico,
a.numero_abonado,
a.tipo_doc_cliente
FROM (SELECT account_num,
UPPER(linea_negocio) AS linea_negocio,
UPPER(segmento) AS segmento,
UPPER(sub_segmento) AS sub_segmento,
num_telefonico,
numero_abonado,
tipo_doc_cliente,
ROW_NUMBER() over (PARTITION BY account_num ORDER BY COALESCE(fecha_baja,current_timestamp()) DESC, fecha_alta DESC) rn
FROM db_desarrollo2021.tmp_mov_parque_completa_csts) a
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA MOVIPARQUE CRUZANDO CON EL CATALOGO PARA TRAER EL CAMPO SEGMENTO
CREATE TABLE db_desarrollo2021.tmp_movparque_seg_csts AS
SELECT a.account_num,
a.linea_negocio,
b.segmento AS segmento,
a.sub_segmento
FROM db_desarrollo2021.tmp_movparque_sin_dup_csts a
LEFT JOIN (SELECT DISTINCT segmento, UPPER(segmentacion) AS segmentacion FROM db_cs_altas.otc_t_homologacion_segmentos) b
ON a.sub_segmento=b.segmentacion;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CRUZADO CON LA MOVIPARQUE PARA OBTENER LINEA NEGOCIO, SEGMENTO Y SUBSEGMENTO
CREATE TABLE db_desarrollo2021.tmp_universo_ppal_mov_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.num_abonado,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
COALESCE(b.segmento,a.segmento) AS segmento,
COALESCE(b.sub_segmento,a.sub_segmento) AS sub_segmento,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
b.linea_negocio
FROM db_desarrollo2021.tmp_trmneqp_full_name_csts a
LEFT JOIN db_desarrollo2021.tmp_movparque_sin_dup_csts b
ON a.account_num=b.account_num;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL PARQUE ANTIGUO
CREATE TABLE db_desarrollo2021.tmp_mov_parque_antiguo_csts AS
SELECT num_telefonico,
account_num,
linea_negocio,
segmento,
sub_segmento,
numero_abonado,
PLAN_CODIGO,
PLAN_NOMBRE,
fecha_alta,
fecha_baja
FROM db_desarrollo2021.tmp_mov_parque_ant_sin_dup_csts
WHERE fecha_alta<'${dia_uno_mes_act_frmt}'
AND (fecha_baja>='${dia_uno_mes_ant_frmt}'
OR fecha_baja IS NULL); 

--CREA TABLA TEMPORAL CON LA INFORMACION DEL PARQUE NUEVO
CREATE TABLE db_desarrollo2021.tmp_mov_parque_nuevo_csts AS
SELECT a.num_telefonico,
a.account_num,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.numero_abonado,
a.PLAN_CODIGO,
a.PLAN_NOMBRE,
a.fecha_alta,
a.fecha_baja
FROM db_desarrollo2021.tmp_mov_parque_sin_dup_csts a
LEFT JOIN (SELECT num_telefonico FROM db_desarrollo2021.tmp_mov_parque_antiguo_csts) b
ON a.num_telefonico=b.num_telefonico
AND b.num_telefonico IS NULL
WHERE fecha_alta>='${dia_uno_mes_act_frmt}';

--CREA TABLA TEMPORAL CON LA INFORMACION DEL CAMPO FUENTE MOVIMIENTO DE ALTAS
CREATE TABLE db_desarrollo2021.tmp_movimientos_bi_csts AS
SELECT fecha_alta AS fecha_movimiento, 
(CASE WHEN UPPER(TRIM(linea_negocio))='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END) AS linea_negocio,
plan_codigo,
nombre_plan,
telefono,
numero_abonado AS num_abonado,
'ALTAS_BI' AS fuente_movimiento,
segmento, 
(CASE WHEN sub_segmento='PEQUENAS' THEN 'PEQUEÑAS' ELSE sub_segmento END) AS sub_segmento
FROM db_cs_altas.otc_t_altas_bi t1
WHERE p_fecha_proceso=${fecha_fin}
UNION ALL
SELECT fecha_transferencia AS fecha_movimiento, 
'POSPAGO' AS linea_negocio, 
codigo_plan_actual AS plan_codigo, 
nombre_plan_actual AS nombre_plan, 
telefono,
subscr_no_actual AS num_abonado,
'PRE_POS_BI' AS fuente_movimiento,
(CASE WHEN UPPER(TRIM(segmento_actual)) = 'CARIBU' THEN 'INDIVIDUAL' ELSE segmento_actual END) AS segmento,
sub_segmento_actual AS sub_segmento
FROM db_cs_altas.otc_t_transfer_in_bi t1
WHERE p_fecha_proceso=${fecha_fin}
UNION ALL
SELECT fecha_transferencia AS fecha_movimiento, 
'PREPAGO' AS linea_negocio,
codigo_plan_actual AS plan_codigo,
nombre_plan_actual AS nombre_plan,
telefono,
subscr_no_actual AS num_abonado,
'POS_PRE_BI' AS fuente_movimiento,
segmento_actual AS segmento,
sub_segmento_actual AS sub_segmento
FROM db_cs_altas.otc_t_transfer_out_bi t1
WHERE p_fecha_proceso=${fecha_fin};

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON INFORMACION CON EL MOVIMIENTO
CREATE TABLE db_desarrollo2021.tmp_trmneqp_movimiento_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
b.fuente_movimiento,
UPPER(COALESCE(b.segmento,a.segmento)) AS segmento,
UPPER(COALESCE(b.sub_segmento,a.sub_segmento)) AS sub_segmento,
b.linea_negocio AS linea_negocio,
b.telefono,
a.num_abonado,
(CASE WHEN b.num_abonado IS NOT NULL OR b.linea_negocio IS NOT NULL 
THEN 1 ELSE 0 END) AS bnd_ln
FROM db_desarrollo2021.tmp_universo_ppal_mov_csts a
LEFT JOIN db_desarrollo2021.tmp_movimientos_bi_csts b
ON a.num_abonado=b.num_abonado
AND a.num_abonado IS NOT NULL;

--CREA TABLA TEMPORAL CON LA INFORMACION DE CUENTAS CON UN SOLO NUMERO TELEFONICO
CREATE TABLE db_desarrollo2021.tmp_cuentas_un_min_csts AS
SELECT account_num, 
COUNT(num_telefonico) AS CANTIDAD_MINES
FROM db_desarrollo2021.tmp_mov_parque_sin_dup_csts
GROUP BY account_num
HAVING COUNT(num_telefonico)=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE MOVIPARQUE SIN MIN
CREATE TABLE db_desarrollo2021.tmp_mines1_cuenta_un_min_csts AS
SELECT (CASE WHEN UPPER(a.linea_negocio)='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END) AS linea_negocio,
a.segmento,
a.sub_segmento,
a.account_num,
a.num_telefonico, 
a.numero_abonado
FROM db_desarrollo2021.tmp_movparque_sin_dup_csts a  
INNER JOIN db_desarrollo2021.tmp_cuentas_un_min_csts b
ON a.account_num=b.account_num;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON INFORMACION DE MOVIMIENTOS CON MINES Y MOV BI
CREATE TABLE db_desarrollo2021.tmp_mov_mines_bi_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,	
a.cruzar_por,	
a.canl_nc,	
a.tienda,	
a.branch,	
a.canal,
a.codigo_da,	
a.fuente_canal,	
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,	
a.nombre_usuario_cruzar,
UPPER(COALESCE(c.fuente_movimiento,a.fuente_movimiento)) AS fuente_movimiento,
UPPER(COALESCE(b.segmento,c.segmento,a.segmento)) AS segmento,
UPPER(COALESCE(b.sub_segmento,c.sub_segmento,a.sub_segmento)) AS sub_segmento,
UPPER(COALESCE(b.linea_negocio,c.linea_negocio,a.linea_negocio)) AS linea_negocio,
COALESCE(b.num_telefonico,c.telefono) AS telefono,
COALESCE(b.numero_abonado,c.num_abonado,a.num_abonado) AS num_abonado,
(CASE WHEN (b.account_num IS NOT NULL OR b.numero_abonado IS NOT NULL) 
AND UPPER(COALESCE(b.linea_negocio,c.linea_negocio,a.linea_negocio)) IS NOT NULL 
THEN 1 ELSE a.bnd_ln END) AS bnd_ln
FROM db_desarrollo2021.tmp_trmneqp_movimiento_csts a
LEFT JOIN db_desarrollo2021.tmp_mines1_cuenta_un_min_csts b
ON a.account_num=b.account_num
LEFT JOIN db_desarrollo2021.tmp_movimientos_bi_csts c
ON a.num_abonado=c.num_abonado
AND a.num_abonado IS NOT NULL;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL PARQUE NUEVO
CREATE TABLE db_desarrollo2021.tmp_fact_mov_pre_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
UPPER(COALESCE(a.linea_negocio,b.linea_negocio,c.linea_negocio)) AS linea_negocio,
(CASE WHEN UPPER(COALESCE(a.segmento,b.segmento,c.segmento))='CARIBU' THEN 'INDIVIDUAL' 
ELSE UPPER(COALESCE(a.segmento,b.segmento,c.segmento)) END) AS segmento,
UPPER(COALESCE(a.sub_segmento,b.sub_segmento,c.sub_segmento)) AS sub_segmento,
COALESCE(a.fuente_movimiento,
(CASE WHEN b.numero_abonado IS NOT NULL THEN 'AAA_ANTERIOR'
WHEN b.numero_abonado IS NULL AND c.numero_abonado IS NOT NULL 
THEN 'AAA_ALTAS' ELSE 'NO DEFINIDO' END)) AS fuente_movimiento,
COALESCE(a.telefono,b.num_telefonico,c.num_telefonico) AS telefono,
(CASE WHEN (b.numero_abonado IS NOT NULL OR c.numero_abonado IS NULL)
AND COALESCE(a.linea_negocio,b.linea_negocio,c.linea_negocio) IS NOT NULL THEN 1 ELSE a.bnd_ln END) AS bnd_ln
FROM db_desarrollo2021.tmp_mov_mines_bi_csts a 
LEFT JOIN db_desarrollo2021.tmp_mov_parque_antiguo_csts b
ON (a.num_abonado=b.numero_abonado
AND a.num_abonado IS NOT NULL)
LEFT JOIN db_desarrollo2021.tmp_mov_parque_nuevo_csts c
ON (a.num_abonado=c.numero_abonado
AND a.num_abonado IS NOT NULL);

--CREA TABLA TEMPORAL CON INFORMACION DE CUENTAS SIN NUMERO TELEFONICO
CREATE TABLE db_desarrollo2021.tmp_cuentas_sin_min_csts AS
SELECT DISTINCT x.account_num
FROM db_desarrollo2021.tmp_fact_mov_pre_csts x
LEFT JOIN (
SELECT DISTINCT a.account_num,a.nulo,b.no_nulo
FROM
(SELECT account_num,count(1) AS nulo FROM db_desarrollo2021.tmp_fact_mov_pre_csts 
WHERE telefono IS NULL
GROUP BY account_num) a
LEFT JOIN (SELECT account_num,count(1) AS no_nulo FROM db_desarrollo2021.tmp_fact_mov_pre_csts 
WHERE telefono IS NOT NULL
GROUP BY account_num) b
ON a.account_num=b.account_num
WHERE b.no_nulo>0) z
ON x.account_num=z.account_num
WHERE z.account_num IS NULL
AND x.telefono IS NULL
AND LENGTH(identificacion_cliente)<=10;

--CREA TABLA TEMPORAL CON LA INFORMACION DE CUENTAS COMPLETA
CREATE TABLE db_desarrollo2021.tmp_cuentas_completa_csts AS
SELECT (CASE WHEN UPPER(a.linea_negocio)='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END) AS linea_negocio,
a.segmento,
a.sub_segmento,
a.account_num,
a.num_telefonico, 
a.numero_abonado
FROM db_desarrollo2021.tmp_movparque_sin_dup_csts a  
INNER JOIN db_desarrollo2021.tmp_cuentas_sin_min_csts b
ON a.account_num=b.account_num;

--CREA TABLA TEMPORAL CON LA ACTUALIZACION DE LOS CAMPOS SEGMENTO, SUBSEGMENTO, LINEA_NEGOCIO, TELEFONO, ABONADO Y bnd_ln
CREATE TABLE db_desarrollo2021.tmp_fact_ppal_completa_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.fuente_movimiento,
(CASE WHEN b.account_num IS NOT NULL THEN 1 ELSE a.bnd_ln END) AS bnd_ln,
UPPER(COALESCE(b.segmento,a.segmento)) AS segmento,
UPPER(COALESCE(b.sub_segmento,a.sub_segmento)) AS sub_segmento,
UPPER(COALESCE(b.linea_negocio,a.linea_negocio)) AS linea_negocio,
COALESCE(b.num_telefonico,a.telefono) AS telefono,
COALESCE(b.numero_abonado,a.num_abonado) AS num_abonado
FROM db_desarrollo2021.tmp_fact_mov_pre_csts a
LEFT JOIN db_desarrollo2021.tmp_cuentas_completa_csts b
ON a.account_num=b.account_num;

--CREA TABLA TEMPORAL CON LA INFORMACION DE TODAS LAS CUENTAS CON 1 O VARIOS NUMEROS DE TELEFONO
CREATE TABLE db_desarrollo2021.tmp_cuenta_segmento_csts AS
SELECT a.linea_negocio,
a.segmento, 
a.sub_segmento, 
a.account_num,
COUNT(a.num_telefonico) AS count_of_num_telefonico
FROM (SELECT (CASE WHEN UPPER(linea_negocio)='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' end) AS linea_negocio, 
segmento, 
sub_segmento, 
account_num,
num_telefonico
FROM db_desarrollo2021.tmp_mov_parque_sin_dup_csts
WHERE UPPER(segmento) IN ('EMPRESA','NEGOCIOS') 
AND linea_negocio IS NOT NULL) a
GROUP BY a.linea_negocio,
segmento,
sub_segmento,
account_num;

--CREA TABLA TEMPORAL CON LA INFORMACION DE TODAS LAS CUENTAS MASIVO
CREATE TABLE db_desarrollo2021.tmp_cuenta_seg_masivo_csts AS
SELECT a.linea_negocio,
a.segmento, 
a.sub_segmento, 
a.account_num,
COUNT(a.num_telefonico) AS count_of_num_telefonico
FROM (SELECT (CASE WHEN UPPER(linea_negocio)='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' end) AS linea_negocio, 
segmento, 
sub_segmento, 
account_num,
num_telefonico
FROM db_desarrollo2021.tmp_mov_parque_sin_dup_csts
WHERE UPPER(segmento) NOT IN ('EMPRESA','NEGOCIOS') 
AND linea_negocio IS NOT NULL) a
GROUP BY a.linea_negocio,
segmento,
sub_segmento,
account_num;

--CREA TABLA TEMPORAL CON LA INFORMACION DE TODAS LAS CUENTAS MASIVO
CREATE TABLE db_desarrollo2021.tmp_universo_fact_mov_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
UPPER(COALESCE(b.linea_negocio,a.linea_negocio)) AS linea_negocio, 
UPPER(COALESCE(TRIM(c.segmento),a.segmento)) AS segmento,
UPPER(COALESCE(TRIM(b.sub_segmento),a.sub_segmento)) as sub_segmento,
--(CASE WHEN a.account_num IS NOT NULL THEN 1 ELSE a.bnd_ln END) AS bnd_ln
bnd_ln
FROM db_desarrollo2021.tmp_fact_ppal_completa_csts a 
LEFT JOIN db_desarrollo2021.tmp_cuenta_segmento_csts b
ON (a.account_num=b.account_num
AND a.linea_negocio IS NULL)
LEFT JOIN db_desarrollo2021.otc_t_ctl_cat_seg_sub_seg c
ON UPPER(TRIM(des_categoria))=UPPER(TRIM(b.sub_segmento));

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE LINEA NEGOCIO, SEGMENTO, SUBSEGMENTO, BND_LN
CREATE TABLE db_desarrollo2021.tmp_universo_fact_mov2_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
COALESCE(UPPER(b.linea_negocio),UPPER(a.linea_negocio)) AS linea_negocio, 
COALESCE(UPPER(TRIM(c.segmento)),UPPER( a.segmento)) AS segmento,
COALESCE(UPPER(TRIM(b.sub_segmento)),UPPER(a.sub_segmento)) AS sub_segmento,
--(CASE WHEN b.account_num IS NOT NULL THEN 1 ELSE bnd_ln END) AS bnd_ln
bnd_ln
FROM db_desarrollo2021.tmp_universo_fact_mov_csts a
LEFT JOIN db_desarrollo2021.tmp_cuenta_seg_masivo_csts b
ON (a.account_num=b.account_num
AND a.linea_negocio IS NULL)
LEFT JOIN db_desarrollo2021.otc_t_ctl_cat_seg_sub_seg c
ON UPPER(TRIM(a.sub_segmento))=UPPER(TRIM(c.des_categoria));

--OBTIENE INFORMACION DE PERIMETROS DEL ANIO Y MES DE ANALISIS
CREATE TABLE db_desarrollo2021.tmp_perimetros_unicos_csts AS
SELECT TRIM(identificador) AS identificador,
TRIM(ejecutivo_asignado) AS ejecutivo_asignado,
TRIM(jefatura) AS jefatura,
TRIM(gerente) AS gerente,
region,
(CASE WHEN segmento='PYMES' THEN 'NEGOCIOS'
WHEN segmento='GRANDES CUENTAS' THEN 'EMPRESAS' ELSE segmento END) AS segmento,
(CASE WHEN area='Empresas' THEN 'EMPRESAS GGCC' ELSE area END) AS canal
FROM db_cs_facturacion.otc_t_perimetros
WHERE anio=${solo_anio} AND mes=${solo_mes} AND identificador <> '1793087353001';

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE CAMPOS Y OBTENCION DE CAMPOS DE PERIMETRO
CREATE TABLE db_desarrollo2021.tmp_fact_mov_perimetro_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
COALESCE(UPPER(b.canal),a.canal) AS canal,
a.codigo_da,
(CASE WHEN b.identificador IS NOT NULL THEN 'PERIMETRO B2B' ELSE a.fuente_canal END) AS fuente_canal,
COALESCE(e.region,a.region) AS region,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
(CASE WHEN a.linea_negocio IS NULL 
AND (CASE WHEN UPPER(TRIM(a.segmento)) IN ('CARIBU','OTROS') THEN 'INDIVIDUAL' ELSE a.segmento END) IS NULL
AND a.sub_segmento IS NULL THEN 'PREPAGO' ELSE a.linea_negocio END) AS linea_negocio,
(CASE WHEN a.linea_negocio IS NULL 
AND (CASE WHEN UPPER(TRIM(a.segmento)) IN ('CARIBU','OTROS') THEN 'INDIVIDUAL' ELSE a.segmento END) IS NULL
AND a.sub_segmento IS NULL THEN 'PREPAGO' ELSE (CASE WHEN UPPER(TRIM(a.segmento)) IN ('CARIBU','OTROS') THEN 'INDIVIDUAL' ELSE a.segmento END) END) AS segmento,
(CASE WHEN a.linea_negocio IS NULL 
AND (CASE WHEN UPPER(TRIM(a.segmento)) IN ('CARIBU','OTROS') THEN 'INDIVIDUAL' ELSE a.segmento END) IS NULL
AND a.sub_segmento IS NULL THEN 'PREPAGO' ELSE a.sub_segmento END) AS sub_segmento,
a.bnd_ln,
a.segmento AS segmento_orig,
e.jefatura AS jefe_perimetro,
e.ejecutivo_asignado AS ejecutivo_perimetro,
e.gerente AS gerente_perimetro
FROM db_desarrollo2021.tmp_universo_fact_mov2_csts a
LEFT JOIN db_desarrollo2021.tmp_perimetros_unicos_csts b
ON (a.identificacion_cliente=b.identificador
AND a.bnd_ln=0)
LEFT JOIN (SELECT ruc FROM db_desarrollo2021.otc_t_catalogo_ruc_das_retail) c
ON (a.identificacion_cliente=c.ruc
AND c.ruc IS NULL)
LEFT JOIN db_desarrollo2021.tmp_perimetros_unicos_csts e
ON a.identificacion_cliente=e.identificador;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE CAMPOS
CREATE TABLE db_desarrollo2021.tmp_fact_mov_update_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
canal,
codigo_da,
fuente_canal,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
fuente_movimiento,
telefono,
(CASE WHEN TRIM(UPPER(linea_negocio))<>'PREPAGO' AND linea_negocio IS NOT NULL THEN 'POSPAGO' ELSE linea_negocio END) AS linea_negocio,
(CASE WHEN (CASE WHEN TRIM(UPPER(sub_segmento))='CARIBU' THEN 'INDIVIDUAL' ELSE TRIM(UPPER(segmento)) END)='GGCC'
THEN 'EMPRESAS' ELSE (CASE WHEN TRIM(UPPER(sub_segmento))='CARIBU' THEN 'INDIVIDUAL' ELSE TRIM(UPPER(segmento)) END) END) AS segmento,
UPPER(sub_segmento) AS sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro
FROM db_desarrollo2021.tmp_fact_mov_perimetro_csts;

--linea_negocio segmeno y sub_segmento
--CREA TABLA TEMPORAL CON LA INFORMACION DE LOS MINES CON SUS PLANES
CREATE TABLE db_desarrollo2021.tmp_mines_planes_csts AS
SELECT a.num_telefonico,
a.plan_codigo,
a.plan_nombre,
b.tarifa_basica
FROM (SELECT num_telefonico,
plan_codigo,
plan_nombre,
(CASE WHEN SUBSTR(plan_codigo,1,2)='NC' THEN SUBSTR(TRIM(plan_codigo),3) ELSE plan_codigo END) AS llave_cruce
FROM db_desarrollo2021.tmp_mov_parque_sin_dup_csts) a
LEFT JOIN db_cs_altas.otc_t_ctl_planes_categoria_tarifa b
ON a.llave_cruce=b.cod_plan_activo;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON EL MOVIMIENTO FINAL Y ACTUALIZA ALGUNOS CAMPOS
CREATE TABLE db_desarrollo2021.tmp_fact_mov_final_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
(CASE WHEN TRIM(a.identificacion_cliente)='1790475247001' THEN 'CELULAR_SEGURO' ELSE a.fuente_movimiento END) AS fuente_movimiento,
a.telefono,
a.linea_negocio, --(COALESCE b y luego a)
(CASE WHEN a.sub_segmento IN ('NEGEMP','NUEVOS NEGOCIOS') 
AND (CASE WHEN a.segmento IN ('EMPRESA','EMPRESAS','GGEE') THEN 'EMPRESAS' ELSE a.segmento END) IS NULL THEN 'NEGOCIOS' 
ELSE (CASE WHEN a.segmento IN ('EMPRESA','EMPRESAS','GGEE') THEN 'EMPRESAS' ELSE a.segmento END) END) AS segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
(CASE WHEN TRIM(a.identificacion_cliente)='1790475247001' THEN 'CELULAR_SEGURO'
WHEN a.linea_negocio='POSPAGO' AND a.fuente_movimiento IN ('AAA_ALTAS','ALTAS_BI','PRE_POS_BI','NO DEFINIDO') THEN 'CONTRATO'
WHEN a.linea_negocio='PREPAGO' AND a.fuente_movimiento IN ('AAA_ALTAS','ALTAS_BI','POS_PRE_BI','NO DEFINIDO') THEN 'PREPAGO'
WHEN a.fuente_movimiento IN ('AAA_ANTERIOR') AND a.linea_negocio='POSPAGO' THEN 'RENOVACION' 
ELSE (CASE WHEN a.linea_negocio='POSPAGO' THEN 'CONTRATO' WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' END) END) AS movimiento,
(CASE WHEN (CASE WHEN TRIM(a.identificacion_cliente)='1790475247001' THEN 'CELULAR_SEGURO' ELSE a.fuente_movimiento END)='AAA_ANTERIOR'
THEN c.plan_codigo ELSE b.plan_codigo END) AS plan_codigo,
(CASE WHEN (CASE WHEN TRIM(a.identificacion_cliente)='1790475247001' THEN 'CELULAR_SEGURO' ELSE a.fuente_movimiento END)='AAA_ANTERIOR'
THEN c.plan_nombre ELSE b.plan_nombre END) AS plan_nombre,
(CASE WHEN (CASE WHEN TRIM(a.identificacion_cliente)='1790475247001' THEN 'CELULAR_SEGURO' ELSE a.fuente_movimiento END)='AAA_ANTERIOR'
THEN d.tarifa_basica ELSE b.tarifa_basica END) AS tarifa_basica
FROM db_desarrollo2021.tmp_fact_mov_update_csts a
LEFT JOIN db_desarrollo2021.tmp_mines_planes_csts b
ON a.telefono=b.num_telefonico
LEFT JOIN db_desarrollo2021.tmp_mov_parque_antiguo_csts c 
ON a.telefono=c.num_telefonico
LEFT JOIN db_cs_altas.otc_t_ctl_planes_categoria_tarifa d 
ON c.plan_codigo=d.cod_plan_activo;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE ALGUNOS CAMPOS
CREATE TABLE db_desarrollo2021.tmp_fact_mov_final_upd_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
canal,
codigo_da,
fuente_canal,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
fuente_movimiento,
telefono,
(CASE WHEN (CASE WHEN linea_negocio='PREPAGO' AND segmento IS NULL THEN 'PREPAGO' ELSE segmento END)='PREPAGO' 
AND segmento NOT IN ('NEGOCIOS','EMPRESAS') THEN 'PREPAGO' ELSE linea_negocio END) AS linea_negocio,
(CASE WHEN (CASE WHEN linea_negocio='PREPAGO' AND segmento IS NULL THEN 'PREPAGO' ELSE segmento END)='PREPAGO' 
AND segmento NOT IN ('NEGOCIOS','EMPRESAS') THEN 'PREPAGO' ELSE segmento END) AS segmento,
(CASE WHEN (CASE WHEN linea_negocio='PREPAGO' AND segmento IS NULL THEN 'PREPAGO' ELSE segmento END)='PREPAGO' 
AND segmento NOT IN ('NEGOCIOS','EMPRESAS') THEN 'PREPAGO' ELSE sub_segmento END) AS sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
(CASE WHEN TRIM(identificacion_cliente)='1790475247001' THEN 'CELULAR SEGURO' ELSE movimiento END) AS movimiento,
plan_codigo,
plan_nombre,
tarifa_basica
FROM db_desarrollo2021.tmp_fact_mov_final_csts;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON ACTUALIZACION DE ALGUNOS CAMPOS
CREATE TABLE db_desarrollo2021.tmp_fact_mov_final_upd1_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
canal,
codigo_da,
fuente_canal,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
fuente_movimiento,
telefono,
(CASE WHEN linea_negocio IS NULL AND movimiento IS NULL THEN 'PREPAGO' ELSE linea_negocio END) AS linea_negocio,
segmento,
sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
(CASE WHEN linea_negocio IS NULL AND movimiento IS NULL THEN 'PREPAGO' ELSE movimiento END) AS movimiento,
plan_codigo,
plan_nombre,
tarifa_basica
FROM db_desarrollo2021.tmp_fact_mov_final_upd_csts;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON COSTOS
CREATE TABLE db_desarrollo2021.tmp_fact_mov_final_costo_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
(CASE WHEN a.codigo_tipo_documento=25 THEN COALESCE(CAST(b.purchase_price AS double),'0.0')*(-1) ELSE COALESCE(CAST(b.purchase_price AS double),'0.0') END) AS costo_unitario,
COALESCE(CAST(b.purchase_price AS double),'0.0')*(-1) AS costo_total,
(CASE WHEN b.imei IS NOT NULL THEN 'COSTOS_AM' END) AS fuente_costo
FROM db_desarrollo2021.tmp_fact_mov_final_upd1_csts a
LEFT JOIN db_desarrollo2021.tmp_imei_articulo_csts b
ON a.imei=b.imei;

--CREA TABLA TEMPORAL CON LA INFORMACION DE COSTO POR MODELO Y FUENTE SIN DUPLICADOS
CREATE TABLE db_desarrollo2021.tmp_costo_x_modelo_csts AS
SELECT a.modelo_terminal,
a.costo_unitario,
a.fecha_factura
FROM (SELECT modelo_terminal,
costo_unitario,
fecha_factura,
row_number() OVER (PARTITION BY modelo_terminal ORDER BY fecha_factura DESC) AS rn
FROM db_desarrollo2021.tmp_fact_mov_final_costo_csts
WHERE codigo_tipo_documento<>25
AND costo_unitario IS NOT NULL
AND costo_unitario>0
AND fuente_costo='COSTOS_AM') a
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE COSTO POR MODELO SIN IMEI
CREATE TABLE db_desarrollo2021.tmp_costo_sin_imei_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_total,
COALESCE(b.costo_unitario,a.costo_unitario) AS costo_unitario,
(CASE WHEN b.modelo_terminal IS NOT NULL THEN 'CALCULADO' ELSE a.fuente_costo END) AS fuente_costo             
FROM db_desarrollo2021.tmp_fact_mov_final_costo_csts a
LEFT JOIN db_desarrollo2021.tmp_costo_x_modelo_csts b
ON TRIM(a.modelo_terminal)=TRIM(b.modelo_terminal)
AND a.costo_unitario IS NULL;

--CREA TABLA TEMPORAL CON LA INFORMACION DE COSTO POR MODELO DEL MES ANTERIOR
CREATE TABLE db_desarrollo2021.tmp_costo_rep_anterior_csts AS
SELECT a.modelo_terminal, 
a.costo_unitario, 
a.fecha_factura
FROM (SELECT modelo_terminal,costo_unitario,fecha_factura,
row_number() OVER (PARTITION BY modelo_terminal ORDER BY fecha_factura DESC) AS rn
FROM db_cs_terminales.otc_t_terminales_simcards
WHERE p_fecha_factura>=${fecha_meses_atras1} AND p_fecha_factura<${fecha_inicio}) a 
LEFT JOIN (SELECT modelo_terminal FROM db_desarrollo2021.tmp_costo_sin_imei_csts
WHERE costo_unitario IS NULL
AND UPPER(modelo_terminal)<>'EQUIPOS') b
ON a.modelo_terminal=b.modelo_terminal
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE COSTO FINAL
CREATE TABLE db_desarrollo2021.tmp_costo_fac_final_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
(CASE WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' AND a.branch LIKE 'AGENTE AE%' THEN 'FRANQUICIA'
WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' 
AND a.branch IN ('CYBERCELL CENTRO HISTORICO','CYBERCELL PLAZA GRANDE','GUAYAS Y 25 DE JUNIO (MOVILEX)','WILLIAMS CERCADO PUYO') THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc='TFN a Distribuidores' AND a.branch IN ('DURMARCELL ENTRE RIOS') THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc='Canal Online' THEN 'CCC- CANAL ON LINE'
WHEN a.canal='OTROS' AND a.canl_nc='Grandes Cuentas' then 'EMPRESAS GGCC'
WHEN a.canal='OTROS' AND a.canl_nc='Puntos de Venta' then 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc IN ('Retencion/Lealtad','Televentas') THEN 'CCC- CALL CENTER'
ELSE a.canal END) AS canal,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
COALESCE(b.costo_unitario,a.costo_unitario) AS costo_unitario,
(CASE WHEN b.modelo_terminal IS NOT NULL THEN 'CALCULADO' ELSE a.modelo_terminal END) AS fuente_costo,
(a.cantidad*(COALESCE(b.costo_unitario,a.costo_unitario))) AS costo_total,
'${ultimo_dia_act_frmt}' AS fecha_proceso 
FROM db_desarrollo2021.tmp_costo_sin_imei_csts a
LEFT JOIN db_desarrollo2021.tmp_costo_rep_anterior_csts b
ON (TRIM(a.modelo_terminal)=TRIM(b.modelo_terminal)
AND a.costo_unitario IS NULL)
WHERE UPPER(a.identificacion_cliente) NOT LIKE '%PRUEBA%';

--CREA TABLA TEMPORAL CON LA INFORMACION DE CUOTA INICIAL Y MONTO FINANCIADO
CREATE TABLE db_desarrollo2021.tmp_cuota_monto_csts AS
SELECT z.account_num, 
z.bill_dtm,
z.invoice_num, 
z.otc_installment_seq,
z.cuota_inicial, 
z.bill_seq,
z.monto_financiado,
row_number() OVER (PARTITION BY z.account_num,z.invoice_num ORDER BY z.account_num ASC,z.invoice_num ASC,z.otc_installment_seq ASC) AS rn
FROM(SELECT a1.account_num, 
a1.bill_dtm,
a1.invoice_num, 
a1.otc_installment_seq,
a1.cuota_inicial, 
a1.bill_seq,
SUM(otc_mny/10000000) AS monto_financiado
FROM (SELECT x.account_num, 
x.bill_dtm,
x.invoice_num,
x.otc_installment_seq, 
x.bill_seq,
SUM(x.monto_cuota_inicial) AS cuota_inicial
FROM (SELECT b.account_num, 
bill_dtm,
c.invoice_num,
a.otc_installment_seq, 
a.bill_seq,
(a.otc_mny/10000000) AS monto_cuota_inicial
FROM db_rbm.otc_t_acchasonetimecharge a
INNER JOIN db_rbm.otc_t_acchasonetimecharge b
ON (a.account_num = b.account_num 
AND a.otc_installment_seq = b.otc_installment_seq)
INNER JOIN db_rbm.otc_t_billsummary c
ON (b.account_num = c.account_num
AND b.bill_seq = c.bill_seq)
WHERE c.actual_bill_dtm>='${fecha_inicio}' AND c.actual_bill_dtm<'${fecha_fin}' 
AND a.installment_number = 0 
AND a.otc_mny > 0 AND a.otc_status = 1
AND b.installment_number IS NULL
AND c.bill_status = 28
AND c.bill_type_id = 9) x
GROUP BY x.account_num, 
x.bill_dtm,
x.invoice_num,
x.otc_installment_seq,
x.bill_seq) a1
INNER JOIN db_rbm.otc_t_acchasonetimecharge b1
ON (a1.account_num = b1.account_num
AND a1.otc_installment_seq = b1.otc_installment_seq)
WHERE b1.installment_number >= 1
AND b1.otc_status = 1
GROUP BY a1.account_num, 
a1.bill_dtm,
a1.invoice_num, 
a1.otc_installment_seq,
a1.cuota_inicial,
a1.bill_seq) z;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL NUMERO DE CUOTAS FINANCIADAS
CREATE TABLE db_desarrollo2021.tmp_cuotas_financiadas_csts AS
SELECT a.account_num,
a.bill_seq,
number_of_installments,
(CASE WHEN number_of_installments IS NULL THEN 1 ELSE number_of_installments END) AS cuotas_financiadas
FROM (SELECT aotc.account_num,
aotc.bill_seq,
MAX(aots.number_of_installments) AS number_of_installments
FROM db_rbm.otc_t_acchasonetimecharge aotc
INNER JOIN db_rbm.otc_t_acchasotcinstlmntsummary aots
ON (aotc.account_num=aots.account_num
AND aotc.otc_installment_seq=aots.otc_installment_seq)
INNER JOIN db_rbm.otc_t_onetimecharge otc
ON aotc.otc_id=otc.otc_id
GROUP BY aotc.account_num,
aotc.bill_seq) a;
			
--CREA TABLA TEMPORAL CON LA INFORMACION DE LA RELACION CONCEPTO ARTICULO
CREATE TABLE db_desarrollo2021.tmp_concepto_articulo_csts AS
SELECT x.concepto_facturable, 
x.codigo_articulo,
x.nombre_articulo
FROM (SELECT concepto_facturable, 
codigo_articulo,
nombre_articulo,
ROW_NUMBER() over (PARTITION BY concepto_facturable ORDER BY concepto_facturable DESC) rn
FROM db_desarrollo2021.tmp_costo_fac_final_csts
WHERE concepto_facturable IS NOT NULL
AND codigo_articulo IS NOT NULL) x
WHERE rn=1;

--CREA TABLA TEMPORAL CON LA INFORMACION DE billsummary PARA OBTENER TODAS LAS SECUENCIAS
--POR CUENTA Y FACTURA
CREATE TABLE db_desarrollo2021.tmp_billsummary_billseq_csts AS
SELECT account_num,invoice_num,bill_seq
FROM db_rbm.otc_t_billsummary
WHERE actual_bill_dtm>='${fecha_inicio}' AND actual_bill_dtm<'${fecha_fin}'
GROUP BY account_num,invoice_num,bill_seq;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON LAS CUOTAS, CANAL Y USUARIOS
CREATE TABLE db_desarrollo2021.tmp_fact_final_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
COALESCE(b.cuotas_financiadas,1) AS cuotas_financiadas,
CAST('' AS string) AS tipo_venta,
(CASE WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' AND a.branch LIKE 'AGENTE AE%' THEN 'FRANQUICIA'
WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' 
AND a.branch IN ('CYBERCELL CENTRO HISTORICO','CYBERCELL PLAZA GRANDE','GUAYAS Y 25 DE JUNIO (MOVILEX)','WILLIAMS CERCADO PUYO') 
THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc='TFN a Distribuidores' AND a.branch IN ('DURMARCELL ENTRE RIOS') THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc='Canal Online' THEN 'CCC- CANAL ON LINE'
WHEN a.canal='OTROS' AND a.canl_nc='Grandes Cuentas' THEN 'EMPRESAS GGCC'
WHEN a.canal='OTROS' AND a.canl_nc='Puntos de Venta' THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc IN ('Retencion/Lealtad','Televentas') THEN 'CCC- CALL CENTER'
ELSE a.canal END) AS canal,
d.nom_distribuidor AS distribuidor,
c.tipo_canal AS tipo_canal
FROM db_desarrollo2021.tmp_costo_fac_final_csts a 
LEFT JOIN db_desarrollo2021.tmp_billsummary_billseq_csts x
ON (a.num_factura=x.invoice_num
AND a.account_num=x.account_num)
LEFT JOIN db_desarrollo2021.tmp_cuotas_financiadas_csts b
ON (a.account_num=b.account_num
AND x.bill_seq=b.bill_seq)
LEFT JOIN db_desarrollo2021.otc_t_catalogo_tipo_canal c
ON a.canal=c.canal
LEFT JOIN db_desarrollo2021.tmp_usuario_cm_csts d
ON UPPER(a.usuario_cruzar)=UPPER(d.usuario);

--CREA TABLA TEMPORAL CON LA INFORMACION DEL UNIVERSO PRINCIPAL CON EL TIPO DE CANAL
CREATE TABLE db_desarrollo2021.tmp_fact_final_tipcanal_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
fuente_canal,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
fuente_movimiento,
telefono,
linea_negocio,
segmento,
sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
canal,
distribuidor,
(CASE WHEN canal='PDVM' AND linea_negocio='PREPAGO' THEN 'INDIRECTO'
WHEN canal='PDVM' AND linea_negocio='POSPAGO' THEN 'DIRECTO'
ELSE tipo_canal END) AS tipo_canal,
cuotas_financiadas
FROM db_desarrollo2021.tmp_fact_final_csts;

--CREA TABLA TEMPORAL CON LA INFORMACION DE CAMPOS PARA NOTAS DE CREDITO
CREATE TABLE db_desarrollo2021.tmp_campos_para_nc_csts AS
SELECT DISTINCT a.account_num,
b.num_factura as nota_credito,
a.fecha_proceso,
a.linea_negocio, 
a.segmento, 
a.sub_segmento, 
a.fuente_movimiento, 
a.num_factura, 
a.num_factura_relacionada, 		  
a.oficina, 
a.usuario, 
a.nombre_usuario, 
a.canal, 
a.fuente_canal, 
a.domain_login_ow, 
a.domain_login_sub, 
a.movimiento, 
a.fuente_costo,
a.despacho,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.usuario_cruzar,
a.nombre_usuario_cruzar,
a.tienda, 
a.branch, 
a.canl_nc,
a.tipo_venta,
a.cuotas_financiadas,
a.tipo_canal,
a.oficina_usuario,
a.ejecutivo_perimetro,
a.jefe_perimetro,
a.distribuidor,
a.gerente_perimetro
FROM (SELECT account_num,
branch,
canal,
canl_nc,
cuotas_financiadas,
CAST('' AS string) AS despacho,
distribuidor,
domain_login_ow,
domain_login_sub,
ejecutivo_perimetro,
CAST(fecha_proceso AS date) AS fecha_proceso,
fuente_canal,
fuente_costo,
fuente_movimiento,
gerente_perimetro,
jefe_perimetro,
linea_negocio,
(CASE WHEN UPPER(movimiento)='RENOVACIÓN' THEN 'RENOVACION' ELSE UPPER(movimiento) END) AS movimiento,
CAST('' AS string) AS	nombre_usuario,
nombre_usuario_cruzar,
num_factura,
origin_invoice_num AS num_factura_relacionada,
oficina,
oficina_usuario,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
segmento,
sub_segmento,
tienda,
tipo_canal,
tipo_venta,
usuario,
usuario_cruzar
FROM db_desarrollo2021.tmp_fact_final_tipcanal_csts
UNION ALL
SELECT account_num,
branch,
UPPER(canal_comercial) AS canal,
CAST('' AS string) AS canl_nc,
cuotas_financiadas,
despacho,
distribuidor_usuario AS distribuidor,
CAST('' AS string) AS domain_login_ow,
CAST('' AS string) AS domain_login_sub,
ejecutivo_perimetro,
fecha_proceso,
UPPER(fuente_canal) AS fuente_canal,
UPPER(fuente_costo) AS fuente_costo,
UPPER(fuente_movimiento) AS fuente_movimiento,
gerente_perimetro,
jefe_perimetro,
linea_negocio,
(CASE WHEN UPPER(movimiento)='RENOVACIÓN' THEN 'RENOVACION' ELSE UPPER(movimiento) END) AS movimiento,
nombre_usuario_factura AS nombre_usuario,
nombre_usuario_final AS nombre_usuario_cruzar,
num_factura,
num_factura_relacionada,
UPPER(oficina) AS oficina,
UPPER(oficina_usuario) AS oficina_usuario,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
(CASE WHEN segmento='Empresa' THEN 'EMPRESAS' ELSE UPPER(segmento) END) AS segmento,
UPPER(sub_segmento) AS sub_segmento,
tienda,
tipo_canal,
tipo_venta,
usuario_factura AS usuario,
usuario_final AS usuario_cruzar
FROM db_desarrollo2021.otc_t_terminales_simcards
WHERE (p_fecha_factura>=${fecha_meses_atras2} AND p_fecha_factura<${fecha_fin})) a 
INNER JOIN db_desarrollo2021.tmp_fact_final_tipcanal_csts b 
ON a.num_factura=b.origin_invoice_num
AND a.account_num=b.account_num
AND b.tipo_documento='NOTA DE CREDITO';

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON LOS CAMPOS DE NOTAS DE CREDITO
CREATE TABLE db_desarrollo2021.tmp_costos_fact_final_v2_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
COALESCE(a.oficina,b.oficina) AS oficina,
COALESCE(a.usuario,b.usuario) AS usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
COALESCE(a.oficina_usuario,b.oficina_usuario) AS oficina_usuario,
COALESCE(a.domain_login_ow,b.domain_login_ow) AS domain_login_ow,
a.domain_name_ow,
COALESCE(a.domain_login_sub,b.domain_login_sub) AS domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
COALESCE(a.fuente_canal,b.fuente_canal) AS fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
COALESCE(a.fuente_movimiento,b.fuente_movimiento) AS fuente_movimiento,
a.telefono,
a.linea_negocio,
COALESCE(a.segmento,b.segmento) AS segmento,
COALESCE(a.sub_segmento,b.sub_segmento) AS sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
COALESCE(a.movimiento,b.movimiento) AS movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
COALESCE(a.fuente_costo,b.fuente_costo) AS fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
COALESCE(a.canal,b.canal) AS canal,
a.distribuidor,
a.tipo_canal,
a.cuotas_financiadas,
b.nombre_usuario AS nombre_usuario
FROM db_desarrollo2021.tmp_fact_final_tipcanal_csts a
LEFT JOIN (SELECT x.account_num,
x.nota_credito,
x.fecha_proceso,
x.linea_negocio,
x.segmento,
x.sub_segmento,
x.fuente_movimiento,
x.num_factura,
x.num_factura_relacionada,
x.oficina,
x.usuario,
x.nombre_usuario,
x.canal,
x.fuente_canal,
x.domain_login_ow,
x.domain_login_sub,
x.movimiento,
x.fuente_costo,
x.despacho,
x.ruc_distribuidor,
x.codigo_plaza,
x.nom_plaza,
x.ciudad,
x.provincia,
x.region,
x.nuevo_subcanal,
x.razon_social,
x.usuario_cruzar,
x.nombre_usuario_cruzar,
x.tienda,
x.branch,
x.canl_nc,
x.tipo_venta,
x.cuotas_financiadas,
x.tipo_canal,
x.oficina_usuario,
x.ejecutivo_perimetro,
x.jefe_perimetro,
x.distribuidor,
x.gerente_perimetro
FROM (SELECT account_num,
nota_credito,
fecha_proceso,
linea_negocio,
segmento,
sub_segmento,
fuente_movimiento,
num_factura,
num_factura_relacionada,
oficina,
usuario,
nombre_usuario,
canal,
fuente_canal,
domain_login_ow,
domain_login_sub,
movimiento,
fuente_costo,
despacho,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
usuario_cruzar,
nombre_usuario_cruzar,
tienda,
branch,
canl_nc,
tipo_venta,
cuotas_financiadas,
tipo_canal,
oficina_usuario,
ejecutivo_perimetro,
jefe_perimetro,
distribuidor,
gerente_perimetro,
row_number() OVER (PARTITION BY nota_credito ORDER BY nota_credito DESC) AS rn
FROM db_desarrollo2021.tmp_campos_para_nc_csts) x
WHERE x.rn=1)  b
ON a.num_factura=b.nota_credito
AND a.account_num=b.account_num;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS CANAL, REGION, FUENTE CANAL
CREATE TABLE db_desarrollo2021.tmp_fact_final_update1_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
(CASE WHEN a.usuario_cruzar='${VAL_USUARIO4}' THEN 'SERVICIO TECNICO' ELSE a.fuente_canal END) AS fuente_canal,
(CASE WHEN a.usuario_cruzar='${VAL_USUARIO4}' THEN 'OTROS' ELSE a.region END) AS region,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
(CASE WHEN a.usuario_cruzar='${VAL_USUARIO4}' THEN 'SERVICIO TECNICO' ELSE a.canal END) AS canal,
a.distribuidor,
a.tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario
FROM db_desarrollo2021.tmp_costos_fact_final_v2_csts a;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS CANAL Y FUENTE CANAL
CREATE TABLE db_desarrollo2021.tmp_fact_final_update2_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
a.canal,
a.distribuidor,
a.tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario
FROM db_desarrollo2021.tmp_fact_final_update1_csts a;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS LINEA NEGOCIO, SEGMENTO, SUBSEGMENTO Y MOVIMIENTO
CREATE TABLE db_desarrollo2021.tmp_fact_final_update3_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
fuente_canal,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
fuente_movimiento,
telefono,
(CASE WHEN canal='RETAIL' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') THEN 'PREPAGO' ELSE linea_negocio END) AS linea_negocio,
(CASE WHEN canal='RETAIL' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') THEN 'PREPAGO' ELSE segmento END) AS segmento,
(CASE WHEN canal='RETAIL' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') THEN 'PREPAGO' ELSE sub_segmento END) AS sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
(CASE WHEN canal='RETAIL' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') THEN 'PREPAGO' ELSE movimiento END) AS movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
canal,
distribuidor,
tipo_canal,
cuotas_financiadas,
nombre_usuario
FROM db_desarrollo2021.tmp_fact_final_update2_csts;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON UPDATE DE CAMPOS LINEA NEGOCIO, SEGMENTO, SUBSEGMENTO Y MOVIMIENTO
CREATE TABLE db_desarrollo2021.tmp_fact_final_update4_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
(CASE WHEN canal='TUENTI' THEN 'NO DEFINIDO' ELSE fuente_canal END) AS fuente_canal,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
region,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
fuente_movimiento,
telefono,
(CASE WHEN linea_negocio='PREPAGO' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') 
AND (movimiento IS NULL OR movimiento<>'RENOVACION') THEN 'PREPAGO' ELSE linea_negocio END) AS linea_negocio,
(CASE WHEN linea_negocio='PREPAGO' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') 
AND (movimiento IS NULL OR movimiento<>'RENOVACION') THEN 'PREPAGO' ELSE segmento END) AS segmento,
(CASE WHEN linea_negocio='PREPAGO' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') 
AND (movimiento IS NULL OR movimiento<>'RENOVACION') THEN 'PREPAGO' ELSE sub_segmento END) AS sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
(CASE WHEN linea_negocio='PREPAGO' AND segmento NOT IN ('EMPRESAS','NEGOCIOS') 
AND (movimiento IS NULL OR movimiento<>'RENOVACION') THEN 'PREPAGO' ELSE movimiento END) AS movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
(CASE WHEN canal='TUENTI' THEN 'OTROS' ELSE canal END) AS canal,
distribuidor,
tipo_canal,
cuotas_financiadas,
nombre_usuario
FROM db_desarrollo2021.tmp_fact_final_update3_csts;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON UPDATE DE LOS CAMPOS REGION, SEGMENTO, MOVIMIENTO, FUENTE MOVIMIENTO
CREATE TABLE db_desarrollo2021.tmp_fact_final_update5_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
fuente_canal,
(CASE WHEN canal IN ('CCC - COMERCIAL CALL CENTER','CCC- CALL CENTER','CCC- CANAL ON LINE','CCC - CANAL ONLINE','CC COMERCIAL CALLCENTER B2B',
'CCC -POSVENTA  EMPRESAS Y NEGOCIOS CALL CENTER','CCC -LEALTAD EMPRESAS Y NEGOCIOS CALL CENTER','CC COMERCIAL ON LINE B2B') 
AND segmento IN ('INDIVIDUAL','PREPAGO') THEN 'NACIONAL' ELSE region END) AS region,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
(CASE WHEN movimiento IS NULL AND linea_negocio='PREPAGO' THEN 'NO DEFINIDO' ELSE fuente_movimiento END) AS fuente_movimiento,
telefono,
linea_negocio,
(CASE WHEN segmento='EMPRESA' THEN 'EMPRESAS' ELSE segmento END) AS segmento,
sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
(CASE WHEN movimiento IS NULL AND linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE movimiento END) AS movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
canal,
distribuidor,
tipo_canal,
cuotas_financiadas,
nombre_usuario
FROM db_desarrollo2021.tmp_fact_final_update4_csts;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON UPDATE DE LOS CAMPOS REGION, MOVIMIENTO, FUENTE MOVIMIENTO
CREATE TABLE db_desarrollo2021.tmp_fact_final_update6_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_cruzar,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
fuente_canal,
(CASE WHEN region IN ('SIERRA CENTRO','SIERRA NORTE') AND segmento IN ('INDIVIDUAL','PREPAGO') THEN 'SIERRA' ELSE region END) AS region,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
nuevo_subcanal,
razon_social,
nombre_usuario_cruzar,
num_abonado,
(CASE WHEN movimiento IS NULL AND linea_negocio='POSPAGO' THEN 'NO DEFINIDO' ELSE fuente_movimiento END) AS fuente_movimiento,
telefono,
linea_negocio,
segmento,
sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
(CASE WHEN movimiento IS NULL AND linea_negocio='POSPAGO' THEN 'CONTRATO' ELSE movimiento END) AS movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
canal,
distribuidor,
tipo_canal,
cuotas_financiadas,
nombre_usuario
FROM db_desarrollo2021.tmp_fact_final_update5_csts;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL DESCARTANDO ALGUNOS REGISTROS
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v2_1_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
a.canal,
a.distribuidor,
a.tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario,
(CASE WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' AND a.branch LIKE 'AGENTE AE%' THEN 'AGENTE ESPECIALIZADO'
WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' 
AND a.branch IN ('CYBERCELL CENTRO HISTORICO','CYBERCELL PLAZA GRANDE','GUAYAS Y 25 DE JUNIO (MOVILEX)','WILLIAMS CERCADO PUYO') THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc='TFN a Distribuidores' AND a.branch='DURMARCELL ENTRE RIOS' THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc='Canal Online' THEN 'CCC- CANAL ON LINE'
WHEN a.canal='OTROS' AND a.canl_nc='Grandes Cuentas' THEN 'EMPRESAS GGCC'
WHEN a.canal='OTROS' AND a.canl_nc='Puntos de Venta' THEN 'DISTRIBUIDOR'
WHEN a.canal='OTROS' AND a.canl_nc IN ('Retencion/Lealtad','Televentas') THEN 'CCC- CALL CENTER'
ELSE a.canal END) llave_canal
FROM db_desarrollo2021.tmp_fact_final_update6_csts a
LEFT JOIN (SELECT DISTINCT num_factura,account_num,nombre_cliente,usuario_cruzar
FROM db_desarrollo2021.tmp_fact_final_update6_csts
WHERE UPPER(nombre_cliente) LIKE '%PRUEBA%'
OR nombre_cliente IN ('JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES MICROEMPRESAS',
'PRUEBASCD3 PRODUCCION',
'Pruebas CD#3 SVC',
'Pruebas CD#3 SVC1',
'JNS//JANUS PRUEBAS FUNCIONALES NUEVOS PREPAGO',
'OTECEL PRUEBAS FUNCIONALIDAD CONTRATO',
'Homenet pruebas 25marzo',
'JNS//JANUS/PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES SOHO',
'PRUEBAS FIX 22 FINALES',
'PRUEBAS FIX 22 TRANSPASO PROPIEDAD',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES MASIVO RIESGO',
'JNS//JANUS PRUEBAS FUNCIONALES NUEVOS EMPRESAS',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES GOLD',
'JANUS PASS PRUEBAS FULLSTACK FULLSTACK',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES MASIVO',
'PRUEBAS TOMS',
'PRUEBAS SEGURO DESGRA',
'PRUEBAS PRODUCCION',
'PRUEBAS CALCULO REGRESIVAS',
'PRUEBAS CD 19',
'CD19PRUEBAS NUEVAS',
'PRUEBAS NUEVAS BASICAS CD19',
'PRUEBAS CONTRATO',
'PRUEBAS BASICAS TOMS FIX20',
'Pruebas SVC',
'Pruebas Svc1',
'PRUEBASSVC .',
'PRUEBAS FIX NUEVAS',
'Pruebas Svc Contrato',
'PRUEBAS FIX21 NEW',
'PRUEBAS SVC JANUS PRODUCCION',
'Pruebas 16Enero',
'Pruebas de Contrato Empresarial Blanco',
'Pruebas Recertificación SVC',
'Pruebas Recertificacion',
'Pruebas SVC2',
'Prueba Suspencion Voluntaria',
'PRUEBA SUSPENSION VOLUNTARIO',
'PRUEBA CONTRATO1',
'PRUEBA FUNCIONAL CONTRATO',
'PRUEBA FINAL FIX21 NEW',
'PRUEBA FINAL OFERTA NUEVO',
'PRUEBA NEW FIX21',
'PRUEBA FIX21 NUEVO',
'Pruebas SVC Alta empresarial pruebas',
'Prueba validacion Nueva fix21',
'CLIENTE PRUEBA',
'JANUS PASS PRUEBAS FULLSTACK FULLSTACK',
'JNS//JANUS PRUEBAS FUNCIONALES NUEVOS EMPRESAS',
'JNS//JANUS/ PRUEBAS FUNCIONALES/ ACC',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES GOLD',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES MASIVO',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES MASIVO MIGRADO',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES MASIVO RIESGO',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES MICROEMPRESAS',
'JNS//JANUS//PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES NEGOCIOS EMPRESAS',
'JNS//JANUS/PRUEBAS FUNCIONALES PRUEBAS FUNCIONALES SOHO',
'PRUEBA SUSPENSION VOLUNTARIO',
'PRUEBA VALIDACION PASOA  PRODUCCION FS',
'PRUEBAS DE FULL STACK PASO A PRODUCCIÓN CREACIÓN DE CLIENTE',
'PRUEBAS JANUS FACTURACION',
'PRUEBAS TOMS',
'Prueba Suspencion Voluntaria',
'RBM PRUEBAS 18.10.2017 DEMP',
'CALIDAD FUNCIONAL 2019',
'CALIDAD FUNCIONAL V2',
'CALIDAD FUNCIONAL V2',
'OTECEL OTECEL1',
'PRUEVAS CALIDAD FUNCIONAL V3')
OR usuario_cruzar IN ('${VAL_USUARIO_FINAL}')
OR UPPER(nombre_cliente) LIKE '%CALIDAD%FUNCIONAL%') b
ON a.num_factura=b.num_factura
AND a.account_num=b.account_num
WHERE b.num_factura IS NULL
AND b.account_num IS NULL;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL ACTUALIZANDO CAMPOS A PARTIR DEL CATALOGO TIPO CANAL Y LA INFORMACION DL ARTICULO
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v3_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_cruzar AS usuario_final,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
(CASE WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' AND a.branch LIKE 'AGENTE AE%' THEN 'FRANQUICIA'
WHEN a.canal='OTROS' AND a.canl_nc='Distribuidor a Cliente Final' AND 
a.branch IN ('CYBERCELL CENTRO HISTORICO','CYBERCELL PLAZA GRANDE','GUAYAS Y 25 DE JUNIO (MOVILEX)','WILLIAMS CERCADO PUYO') THEN 'BRANCH'
WHEN a.canal='OTROS' AND a.canl_nc='TFN a Distribuidores' AND a.branch='DURMARCELL ENTRE RIOS' THEN 'BRANCH'
WHEN a.canal='OTROS' AND a.canl_nc='Canal Online' THEN 'CANAL_NETCRACKER'
WHEN a.canal='OTROS' AND a.canl_nc='Grandes Cuentas' then 'CANAL_NETCRACKER'
WHEN a.canal='OTROS' AND a.canl_nc='Puntos de Venta' then 'CANAL_NETCRACKER'
WHEN a.canal='OTROS' AND a.canl_nc IN ('Retencion/Lealtad','Televentas') THEN 'CANAL_NETCRACKER'
ELSE a.fuente_canal END) AS fuente_canal,
(CASE WHEN a.canal='OTROS' AND a.canl_nc IN ('Retencion/Lealtad','Televentas','Canal Online') THEN 'NACIONAL' 
ELSE a.region END) AS region,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_cruzar AS nombre_usuario_final,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
a.llave_canal AS canal,
a.distribuidor,
(CASE WHEN a.llave_canal='PDVM' AND a.linea_negocio='PREPAGO' THEN 'INDIRECTO'
WHEN a.llave_canal='PDVM' AND a.linea_negocio='POSPAGO' THEN 'DIRECTO' ELSE a.tipo_canal END) AS tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario
FROM db_desarrollo2021.tmp_costo_fact_final_v2_1_csts a
LEFT JOIN db_desarrollo2021.otc_t_catalogo_tipo_canal b
ON a.llave_canal=b.canal
LEFT JOIN db_desarrollo2021.tmp_concepto_articulo_csts c
ON a.concepto_facturable=c.concepto_facturable;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL ACTUALIZANDO ALGUNOS CAMPOS
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v4_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
COALESCE(a.codigo_articulo,d.codigo_articulo) AS codigo_articulo,
COALESCE(a.nombre_articulo,D.nombre_articulo) AS nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_final,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
a.fuente_canal,
(CASE WHEN a.segmento NOT IN ('EMPRESAS','NEGOCIOS') 
AND (CASE WHEN a.canal IN ('CAV PROPIO','CAVS PROPIO') THEN 'CAV PROPIO'
WHEN a.segmento NOT IN ('EMPRESAS','NEGOCIOS') AND a.canal='DISTRIBUIDOR'
AND a.identificacion_cliente='1792161037001' THEN 'AGENTE ESPECIALIZADO'
WHEN a.canal IS NULL THEN b.canal_venta ELSE a.canal END)='OTROS' THEN 'OTROS'
WHEN a.segmento NOT IN ('EMPRESAS','NEGOCIOS') AND a.usuario_final='${VAL_USUARIO4}' THEN 'OTROS'
WHEN a.segmento NOT IN ('EMPRESAS','NEGOCIOS') AND a.canal='DISTRIBUIDOR' 
AND a.identificacion_cliente='1792161037001' THEN 'SIERRA'
WHEN a.region IS NULL THEN b.region ELSE a.region END) AS region,
c.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.nuevo_subcanal,
a.razon_social,
UPPER(a.nombre_usuario_final) AS nombre_usuario_final,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
(CASE WHEN a.canal IN ('CAV PROPIO','CAVS PROPIO') THEN 'CAV PROPIO'
WHEN a.segmento NOT IN ('EMPRESAS','NEGOCIOS') AND a.canal='DISTRIBUIDOR'
AND a.identificacion_cliente='1792161037001' THEN 'AGENTE ESPECIALIZADO'
WHEN a.canal IS NULL THEN b.canal_venta ELSE a.canal END) AS canal,
(CASE WHEN a.canal IS NULL THEN b.tipo_canal_venta ELSE a.tipo_canal END) AS tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario,
c.nom_distribuidor AS distribuidor,
e.nrc_base_price AS precio_base,
e.nrc_ov_price AS precio_con_override,
e.nrc_ov_created_by AS usuario_override,
CAST(SUBSTR(e.nrc_ov_created_when,1,10) AS date) AS fecha_override,
'NO' AS nota_credito_masiva
FROM db_desarrollo2021.tmp_costo_fact_final_v3_csts a
LEFT JOIN (SELECT x.nombre_oficina_venta, 
x.tipo_canal_venta,
x.canal_venta,
x.region
FROM (SELECT nombre_oficina_venta, 
tipo_canal_venta,
canal_venta,
region,
ROW_NUMBER() over (PARTITION BY nombre_oficina_venta ORDER BY nombre_oficina_venta DESC) rn
FROM db_desarrollo2021.otc_t_asigna_canal_ventas) x
WHERE rn=1) b
ON UPPER(a.nombre_cliente)=UPPER(b.nombre_oficina_venta)
LEFT JOIN db_desarrollo2021.tmp_usuario_cm_csts c
ON UPPER(a.usuario_final)=UPPER(c.usuario)
LEFT JOIN db_desarrollo2021.tmp_concepto_articulo_csts d
ON a.concepto_facturable=d.concepto_facturable
LEFT JOIN db_rdb.otc_t_overwrite_equipos e
ON (a.orden_de_venta = e.orden_name 
AND a.telefono = e.phone_number 
AND a.imei IS NOT NULL);

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL ACTUALIZANDO LOS CAMPOS DE REGION, FUENTE CANAL Y CANAL
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v4up_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_final,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
(CASE WHEN usuario_final='${VAL_USUARIO4}' THEN 'SERVICIO TECNICO' ELSE fuente_canal END) AS fuente_canal,
(CASE WHEN usuario_final='${VAL_USUARIO4}' THEN 'OTROS' ELSE
(CASE WHEN ((CASE WHEN region IS NULL AND canal IS NOT NULL AND segmento NOT IN ('EMPRESAS','NEGOCIOS') THEN 'OTROS' ELSE region END) 
IS NULL OR (CASE WHEN region IS NULL AND canal IS NOT NULL AND segmento NOT IN ('EMPRESAS','NEGOCIOS') THEN 'OTROS' ELSE region END)
='OTROS CANALES') AND segmento IN ('EMPRESAS','NEGOCIOS') THEN 'OTROS' ELSE 
(CASE WHEN region IS NULL AND canal IS NOT NULL AND segmento NOT IN ('EMPRESAS','NEGOCIOS') THEN 'OTROS' ELSE region END) END)
END) AS region,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
nuevo_subcanal,
razon_social,
nombre_usuario_final,
num_abonado,
fuente_movimiento,
telefono,
linea_negocio,
segmento,
sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
(CASE WHEN identificacion_cliente='0391007160001' AND clasificacion='TERMINALES' THEN 'DISTRIBUIDOR' ELSE 
(CASE WHEN usuario_final='NA002828' THEN 'SERVICIO TECNICO' ELSE canal END) 
END) AS canal,
tipo_canal,
cuotas_financiadas,
nombre_usuario,
distribuidor,
precio_base,
precio_con_override,
usuario_override,
fecha_override,
nota_credito_masiva
FROM db_desarrollo2021.tmp_costo_fact_final_v4_csts;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL ACTUALIZANDO LOS CAMPOS DE REGION, CANAL, SEGMENTO Y TIPO VENTA
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v5up_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_final,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
fuente_canal,
(CASE WHEN num_factura='056-332-000332657' THEN 'OTROS' ELSE
(CASE WHEN distribuidor='ONLINE_INTERNO' THEN 'NACIONAL' ELSE region END) 
END) AS region,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
nuevo_subcanal,
razon_social,
nombre_usuario_final,
num_abonado,
fuente_movimiento,
telefono,
linea_negocio,
(CASE WHEN identificacion_cliente IN ('1792141850001','1791415728001','0391007160001','1792306043001','1091717898001','1792504015001') THEN 'PREPAGO' ELSE
(CASE WHEN identificacion_cliente IN ('1793087353001','1793158323001') THEN 'PREPAGO' ELSE segmento END)
END) AS segmento,
sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
(CASE WHEN num_factura='056-332-000332657' THEN 'OTROS' ELSE
(CASE WHEN distribuidor='ONLINE_INTERNO' THEN 'CCC- CANAL ON LINE' ELSE canal END)
END) AS canal,
tipo_canal,
cuotas_financiadas,
nombre_usuario,
distribuidor,
precio_base,
precio_con_override,
usuario_override,
fecha_override,
nota_credito_masiva
FROM db_desarrollo2021.tmp_costo_fact_final_v4up_csts;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL ACTUALIZANDO LOS CAMPOS REGION Y CANAL
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v5_csts AS
SELECT fecha_factura,
bill_status,
sri_authorization_date,
codigo_tipo_documento,
tipo_documento,
origin_doc_type_id,
documento,
num_factura,
origin_invoice_num,
office_code,
oficina,
usuario,
account_num,
nombre_cliente,
customer_id_type,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
imei,
cantidad,
monto,
domain_login,
descripcion_gama,
2g_3g,
modelo_scl,
clasificacion,
cod_categoria,
forma_pago_factura,
tipo_movimiento_mes,
fecha_alta,
num_telefonico,
codigo_articulo,
nombre_articulo,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
band,
oficina_usuario,
domain_login_ow,
domain_name_ow,
domain_login_sub,
domain_name_sub,
orden_de_venta,
created_by,
created_when,
usuario_factura,
nombre_usuario_factura,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
usuario_final,
cruzar_por,
canl_nc,
tienda,
branch,
codigo_da,
fuente_canal,
(CASE WHEN canal IN ('CCC - COMERCIAL CALL CENTER','CCC- CALL CENTER','CCC- CANAL ON LINE') 
AND segmento IN ('INDIVIDUAL','PREPAGO') THEN 'NACIONAL' ELSE region END) AS region,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
nuevo_subcanal,
razon_social,
nombre_usuario_final,
num_abonado,
fuente_movimiento,
telefono,
linea_negocio,
segmento,
sub_segmento,
bnd_ln,
segmento_orig,
jefe_perimetro,
ejecutivo_perimetro,
gerente_perimetro,
movimiento,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
fuente_costo,
costo_total,
fecha_proceso,
tipo_venta,
(CASE WHEN identificacion_cliente IN ('1792141850001','1791415728001','0391007160001','1792306043001','1091717898001','1792504015001') 
THEN 'ZONIFICADOS' ELSE canal END) AS canal,
tipo_canal,
cuotas_financiadas,
nombre_usuario,
distribuidor,
precio_base,
precio_con_override,
usuario_override,
fecha_override,
nota_credito_masiva
FROM db_desarrollo2021.tmp_costo_fact_final_v5up_csts;

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA TARJETA BANCO
CREATE TABLE db_desarrollo2021.tmp_tarjeta_banco_csts AS
SELECT DISTINCT a.account_num,
a.account_payment_mny/10000000 as valor,
b.bank_name as tarjeta_banco,
b.invoice_prefix as factura,
b.total_installments as num_cuotas,
b.identification_num,
b.bank_account_holder_name
FROM db_rbm.otc_t_accountpayment a
INNER JOIN db_rbm.otc_t_accountpayattributes b
ON a.account_num=b.account_num
AND a.account_payment_seq=b.account_payment_seq
AND b.bank_name<>'INTERDIN 1 (SSP)'
AND b.invoice_prefix IS NOT NULL
INNER JOIN db_rbm.otc_t_physicalpayment c
ON a.customer_ref=c.customer_ref
AND a.physical_payment_seq=c.physical_payment_seq
INNER JOIN db_rbm.otc_t_accountattributes d
ON a.account_num=d.account_num
INNER JOIN db_rbm.otc_t_paymentmethod e
ON c.payment_method_id=e.payment_method_id
AND e.payment_method_id=1
WHERE a.created_dtm_date>=${fecha_inicio} AND a.created_dtm_date<${fecha_fin}
AND b.pt_fecha>=${fecha_inicio} AND b.pt_fecha<${fecha_fin}
AND c.created_dtm_date>=${fecha_inicio} AND c.created_dtm_date<${fecha_fin};

--CREA TABLA TEMPORAL CON LA INFORMACION DE LA TARJETA DE PAGO
CREATE TABLE db_desarrollo2021.tmp_tarjeta_banco1_csts AS
SELECT DISTINCT factura,tarjeta_banco FROM db_desarrollo2021.tmp_tarjeta_banco_csts WHERE factura IS NOT NULL;

--CREA TABLA TEMPORAL CON EL PIVOT DE LA INFORMACION DE LA TARJETA BANCO
CREATE TABLE db_desarrollo2021.tmp_pivot_tarjeta_banco_csts AS
SELECT a.factura,a.tarjeta_banco AS columna1,b.tarjeta_banco AS columna2,c.tarjeta_banco AS columna3
FROM(
(SELECT DISTINCT factura,tarjeta_banco,row_number() over (partition by factura order by factura DESC) rn
FROM db_desarrollo2021.tmp_tarjeta_banco1_csts) a
LEFT JOIN (SELECT DISTINCT factura,tarjeta_banco,row_number() over (partition by factura order by factura DESC) rn
FROM db_desarrollo2021.tmp_tarjeta_banco1_csts) b
ON a.factura=b.factura AND a.rn=1 AND b.rn=2
LEFT JOIN (SELECT DISTINCT factura,tarjeta_banco,row_number() over (partition by factura order by factura DESC) rn
FROM db_desarrollo2021.tmp_tarjeta_banco1_csts) c
ON a.factura=c.factura AND a.rn=1 AND c.rn=3)
WHERE a.rn=1;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON LA INFORMACION DEL MONTO FINANCIADO SIN IVA Y LA TARJETA BANCO
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v6_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
a.nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_final,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_final,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
a.segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
a.canal,
a.tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario,
a.distribuidor,
a.precio_base,
a.precio_con_override,
a.usuario_override,
a.fecha_override,
a.nota_credito_masiva,
b.cuota_inicial,
b.monto_financiado,
(a.monto-b.cuota_inicial) AS financiado_sin_iva,
LENGTH(a.identificacion_cliente) AS registro,
c.columna1 as tarjeta_banco, 
c.columna2 as tarjeta_banco2, 
c.columna3 as tarjeta_banco3,
c.factura 
FROM db_desarrollo2021.tmp_costo_fact_final_v5_csts a
LEFT JOIN db_desarrollo2021.tmp_cuota_monto_csts b
ON (a.num_factura=b.invoice_num 
AND a.account_num=b.account_num
AND b.rn=1)
LEFT JOIN db_desarrollo2021.tmp_pivot_tarjeta_banco_csts c
ON a.num_factura=c.factura;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON CALCULO DE ALGUNOS CAMPOS Y ASIGNACION DE MONTOS Y TARJETA
CREATE TABLE db_desarrollo2021.tmp_costo_fact_final_v4_1_csts AS
SELECT DISTINCT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
nombre_cliente,
a.customer_id_type,
a.identificacion_cliente,
a.concepto_facturable,
(CASE WHEN a.modelo_guia_comercial='IPHONE SE 2020 64GB' THEN 'IPHONE SE 2020 64GB' ELSE a.modelo_terminal END) AS modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
(CASE WHEN a.modelo_guia_comercial='APORTE FABRICANTES' THEN 'APORTE FABRICANTES' 
WHEN a.modelo_guia_comercial='IPHONE SE 2020 64GB' THEN 'TERMINALES' 
WHEN a.segmentacion_smarts='ACCESORIOS' THEN 'ACCESORIOS'
WHEN a.segmentacion_smarts='HOGAR INTELIGENTE' OR a.modelo_terminal='DHI NEXXT CÁMARA INTELIGENTE  MOTORIZADA' THEN 'HOGAR INTELIGENTE'
WHEN a.concepto_facturable IN ('19422','19421') THEN 'TARJETAS' 
WHEN a.modelo_terminal='CARGO POR ENVÍO A DOMICILIO' THEN 'DELIVERY' ELSE a.clasificacion END) AS clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
(CASE WHEN a.concepto_facturable='18875' THEN 'DELIVERY SIMCARD'
WHEN a.concepto_facturable='6212' THEN 'DELIVERY TERMINALES' ELSE a.clasificacion_terminal END) AS clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_final,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_final,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio, 
(CASE WHEN a.segmento='GGCC' THEN 'EMPRESAS'
WHEN a.segmentacion_smarts IN ('HOGAR INTELIGENTE','ACCESORIOS') OR a.modelo_terminal='DHI NEXXT CÁMARA INTELIGENTE  MOTORIZADA' THEN 'INDIVIDUAL'
WHEN a.telefono IS NULL AND a.clasificacion='TERMINALES' AND a.tipo_cargo='CARGO' AND a.canal='CCC - CANAL ONLINE' AND a.plan_codigo IS NULL AND a.registro=10 THEN 'INDIVIDUAL' 
ELSE a.segmento END) AS segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
(CASE WHEN a.telefono IS NULL AND a.clasificacion='TERMINALES' AND a.tipo_cargo='CARGO' AND a.canal='CCC - CANAL ONLINE' AND a.plan_codigo IS NULL
AND a.registro=10 THEN 'EQUIPO_LIBRE' ELSE a.movimiento END) AS movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
a.tipo_venta,
a.canal,
a.tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario,
a.distribuidor,
a.precio_base,
a.precio_con_override,
a.usuario_override,
a.fecha_override,
a.nota_credito_masiva,
a.cuota_inicial,
a.monto_financiado,
a.financiado_sin_iva,
a.registro,
a.tarjeta_banco, 
c.num_cuotas as cuotas,
a.tarjeta_banco2, 
d.num_cuotas as cuotas2,
a.tarjeta_banco3,
e.num_cuotas as cuotas3,
a.factura
FROM db_desarrollo2021.tmp_costo_fact_final_v6_csts a
LEFT JOIN db_desarrollo2021.tmp_tarjeta_banco_csts c
ON (a.num_factura=c.factura
AND a.tarjeta_banco=c.tarjeta_banco)
LEFT JOIN db_desarrollo2021.tmp_tarjeta_banco_csts d
ON (a.num_factura=d.factura
AND a.tarjeta_banco2=d.tarjeta_banco)
LEFT JOIN db_desarrollo2021.tmp_tarjeta_banco_csts e
ON (a.num_factura=e.factura
AND a.tarjeta_banco3=e.tarjeta_banco)
WHERE a.codigo_tipo_documento<>2;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON ACTUALIZACION EN ALGUNOS CAMPOS
CREATE TABLE db_desarrollo2021.tmp_costo_fact_exporta_csts AS
SELECT a.fecha_factura,
a.bill_status,
a.sri_authorization_date,
a.codigo_tipo_documento,
a.tipo_documento,
a.origin_doc_type_id,
a.documento,
a.num_factura,
a.origin_invoice_num,
a.office_code,
a.oficina,
a.usuario,
a.account_num,
regexp_replace(a.nombre_cliente,'  ','') AS nombre_cliente,
(CASE WHEN a.tipo_documento='NOTA DE CREDITO' THEN a.customer_id_type ELSE '' END) AS tipo_doc_cliente,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.imei,
a.cantidad,
a.monto,
a.domain_login,
a.descripcion_gama,
a.2g_3g,
a.modelo_scl,
a.clasificacion,
a.cod_categoria,
a.forma_pago_factura,
a.tipo_movimiento_mes,
a.fecha_alta,
a.num_telefonico,
a.codigo_articulo,
a.nombre_articulo,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.band,
a.oficina_usuario,
a.domain_login_ow,
a.domain_name_ow,
a.domain_login_sub,
a.domain_name_sub,
a.orden_de_venta,
a.created_by,
a.created_when,
a.usuario_factura,
a.nombre_usuario_factura,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.usuario_final,
a.cruzar_por,
a.canl_nc,
a.tienda,
a.branch,
a.codigo_da,
a.fuente_canal,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.region,
a.nuevo_subcanal,
a.razon_social,
a.nombre_usuario_final,
a.num_abonado,
a.fuente_movimiento,
a.telefono,
a.linea_negocio,
(CASE WHEN a.segmento IS NULL THEN pu.segmento ELSE a.segmento END) AS segmento,
a.sub_segmento,
a.bnd_ln,
a.segmento_orig,
a.jefe_perimetro,
a.ejecutivo_perimetro,
a.gerente_perimetro,
a.movimiento,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
(CASE WHEN a.codigo_tipo_documento=25 THEN (a.costo_unitario*-1) ELSE a.costo_unitario END) AS costo_unitario,
a.fuente_costo,
a.costo_total,
a.fecha_proceso,
(CASE WHEN a.identificacion_cliente IN ('1793087353001','1793158323001') THEN 'FINANCIAMIENTO TERCEROS'
WHEN a.tipo_documento IN ('Factura Afecta Cuota','Factura Miscelánea Afecta Cuota') THEN 'CREDITO DIRECTO'
ELSE 'No Definido' END) AS tipo_venta,
a.canal,
a.tipo_canal,
a.cuotas_financiadas,
a.nombre_usuario,
a.distribuidor,
a.precio_base,
a.precio_con_override,
a.usuario_override,
a.fecha_override,
a.nota_credito_masiva,
a.cuota_inicial,
a.monto_financiado,
a.financiado_sin_iva,
a.registro,
a.tarjeta_banco,
a.cuotas,
a.tarjeta_banco2,
a.cuotas2,
a.tarjeta_banco3,
a.cuotas3,
a.factura,
(CASE WHEN a.codigo_tipo_documento=25 THEN (ABS(a.monto/a.cantidad)*-1) ELSE (a.monto/a.cantidad) END) AS monto_unitario,
(CASE WHEN a.codigo_tipo_documento=25 THEN (ABS(a.monto/a.cantidad)*-1) - (a.costo_unitario*-1) 
ELSE ((a.monto/a.cantidad) - a.costo_unitario) END) AS subsidio_unitario,
a.canl_nc AS canal_netcracker,
COALESCE(a.origin_invoice_num,b.num_factura_relacionada) AS num_factura_relacionada, 
b.fechafacturarelacionada AS fecha_factura_relacionada,
(CASE WHEN identificacion_cliente IN('0391007160001','0190495825001') THEN 'ZONIFICADOS'
WHEN identificacion_cliente IN('0990633436001','0990017514001') THEN 'RETAIL'
WHEN (CASE WHEN c.segmento IS NULL THEN a.segmento ELSE c.segmento END) IS NULL THEN pu.segmento 
ELSE (CASE WHEN c.segmento IS NULL THEN a.segmento ELSE c.segmento END) END) AS segmento_final,
(DATEDIFF(fecha_factura,(CASE WHEN a.fecha_alta IS NULL THEN CAST('2005-01-01 00:00:00' AS TIMESTAMP) ELSE a.fecha_alta END))/30) AS antiguedad_meses
FROM db_desarrollo2021.tmp_costo_fact_final_v4_1_csts a
LEFT JOIN db_rbm.otc_t_casca_fac_relacionada b
ON a.num_factura=b.nota_credito 
AND a.account_num=b.account_num_nc
AND b.fechafactura>=${fecha_inicio} AND b.fechafactura<${fecha_fin}
LEFT JOIN db_desarrollo2021.otc_t_ctl_seg_terminal c
ON a.identificacion_cliente=c.ruc
LEFT JOIN db_desarrollo2021.tmp_perimetros_unicos_csts pu
ON a.identificacion_cliente=pu.identificador;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON ACTUALIZACION DE CAMPOS PARA CUADRES
CREATE TABLE db_desarrollo2021.tmp_costo_fact_exporta_otra_csts AS
SELECT (CASE WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END) AS linea_negocio,
(CASE WHEN a.segmento IS NULL AND (CASE WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END)='POSPAGO' THEN 'SIN SEGMENTO' 
WHEN a.segmento IS NULL AND (CASE WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END)='PREPAGO' THEN 'PREPAGO'
ELSE a.segmento END) AS segmento,
a.sub_segmento,
(CASE WHEN a.movimiento='EQUIPO_LIBRE' THEN 'EQUIPO LIBRE' ELSE a.movimiento END) AS movimiento,
a.fuente_movimiento,
a.telefono,
a.clasificacion,
a.num_factura,
a.num_factura_relacionada,
(CASE WHEN a.fecha_factura_relacionada IS NULL THEN NULL ELSE
CONCAT(SUBSTR(a.fecha_factura_relacionada,1,4),'-',SUBSTR(a.fecha_factura_relacionada,5,2),'-',SUBSTR(a.fecha_factura_relacionada,7,2)) END) AS fecha_factura_relacionada,
CAST(a.fecha_factura AS date) AS fecha_factura,
a.usuario_factura,
a.nombre_usuario_factura,
a.usuario_final,
a.nombre_usuario_final,
a.oficina_usuario,
a.distribuidor AS distribuidor_usuario,
a.codigo_tipo_documento,
a.tipo_documento,
a.oficina,
a.account_num,
a.nombre_cliente,
(CASE WHEN a.tipo_doc_cliente='Cédula' THEN 'Cedula' ELSE a.tipo_doc_cliente END) AS tipo_doc_cliente,
a.identificacion_cliente,
CAST(a.concepto_facturable AS string) AS concepto_facturable,
a.modelo_terminal,
a.codigo_articulo,
a.nombre_articulo AS descripcion_articulo,
a.imei,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.codigo_da,
'' AS despacho,
a.razon_social,
(CASE WHEN a.canal='CAVS FRANQUICIA' THEN 'AGENTE ESPECIALIZADO' ELSE a.canal END) AS canal_comercial,
a.tipo_canal,
a.fuente_canal,
a.region,
a.cantidad,
a.monto,
a.monto_unitario,
a.num_abonado,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
CAST(a.costo_unitario AS double) AS costo_unitario,
a.costo_total,
a.precio_base AS pvp_prepago,
a.subsidio_unitario,
a.fuente_costo,
CAST(a.fecha_proceso AS date) AS fecha_proceso,
a.tienda,
a.branch,
a.canal_netcracker,
CAST(a.cuotas_financiadas AS double) AS cuotas_financiadas,
(CASE WHEN 
(CASE WHEN a.tipo_venta='No Definido' THEN
(CASE WHEN (a.tipo_documento IN ('Factura Contado','Factura Miscelánea') AND a.tarjeta_banco IS NOT NULL) THEN 'TARJETA'
ELSE 'CONTADO' END) 
ELSE a.tipo_venta END)
='FINANCIAMIENTO TERCEROS' THEN 'FINANCIAMIENTOS TERCEROS' ELSE
(CASE WHEN a.tipo_venta='No Definido' THEN
(CASE WHEN (a.tipo_documento IN ('Factura Contado','Factura Miscelánea') AND a.tarjeta_banco IS NOT NULL) THEN 'TARJETA'
ELSE 'CONTADO' END) 
ELSE a.tipo_venta END)
END) AS tipo_venta,
a.ejecutivo_perimetro,
a.jefe_perimetro,
a.gerente_perimetro,
a.orden_de_venta AS orden_venta,
a.created_when AS fecha_creacion_orden_venta,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.nota_credito_masiva,
a.precio_base,
a.precio_con_override,
a.usuario_override,
a.fecha_override,
a.cuota_inicial,
a.monto_financiado,
a.financiado_sin_iva,
a.forma_pago_factura,
a.tarjeta_banco,
(CASE WHEN a.cuotas IS NULL THEN '1' ELSE a.cuotas END) AS cuotas,
a.tarjeta_banco2,
a.cuotas2,
a.tarjeta_banco3,
a.cuotas3,
(CASE WHEN (CASE WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE a.segmento_final END) IS NULL
THEN (CASE WHEN a.segmento IS NULL AND (CASE WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END)='POSPAGO' THEN 'SIN SEGMENTO' 
WHEN a.segmento IS NULL AND (CASE WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE 'POSPAGO' END)='PREPAGO' THEN 'PREPAGO'
ELSE a.segmento END) 
ELSE (CASE WHEN a.linea_negocio='PREPAGO' THEN 'PREPAGO' ELSE a.segmento_final END) END) AS segmento_final,
a.tipo_movimiento_mes,
a.fecha_alta,
(CASE WHEN a.antiguedad_meses<0 THEN 0 ELSE floor(a.antiguedad_meses) END) antiguedad_meses,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.nuevo_subcanal,
CAST(from_unixtime(unix_timestamp(a.fecha_factura,'yyyy-MM-dd'),'yyyyMMdd') AS INT) AS p_fecha_factura
FROM db_desarrollo2021.tmp_costo_fact_exporta_csts a;

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL CON IDs
CREATE TABLE db_desarrollo2021.tmp_costo_fact_exporta_otra1_csts AS
SELECT (CASE WHEN a.segmento_final IN ('RETAIL','ZONIFICADOS','CREDICEL','PAYJOY') THEN 'PREPAGO' ELSE linea_negocio END) AS linea_negocio,
a.segmento,
a.sub_segmento,
a.movimiento,
a.fuente_movimiento,
a.telefono,
a.clasificacion,
a.num_factura,
a.num_factura_relacionada,
a.fecha_factura_relacionada,
a.fecha_factura,
a.usuario_factura,
a.nombre_usuario_factura,
a.usuario_final,
a.nombre_usuario_final,
a.oficina_usuario,
a.distribuidor_usuario,
a.codigo_tipo_documento,
a.tipo_documento,
a.oficina,
a.account_num,
a.nombre_cliente,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.codigo_articulo,
a.descripcion_articulo,
a.imei,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.codigo_da,
a.despacho,
a.razon_social,
a.canal_comercial,
a.tipo_canal,
a.fuente_canal,
a.region,
a.cantidad,
a.monto,
a.monto_unitario,
a.num_abonado,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.costo_total,
a.pvp_prepago,
a.subsidio_unitario,
a.fuente_costo,
a.fecha_proceso,
a.tienda,
a.branch,
a.canal_netcracker,
a.cuotas_financiadas,
a.tipo_venta,
a.ejecutivo_perimetro,
a.jefe_perimetro,
a.gerente_perimetro,
a.orden_venta,
a.fecha_creacion_orden_venta,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.nota_credito_masiva,
a.precio_base,
a.precio_con_override,
a.usuario_override,
a.fecha_override,
a.cuota_inicial,
a.monto_financiado,
a.financiado_sin_iva,
a.forma_pago_factura,
a.tarjeta_banco,
a.cuotas,
a.tarjeta_banco2,
a.cuotas2,
a.tarjeta_banco3,
a.cuotas3,
a.segmento_final,
d.id_tipo_movimiento AS id_canal,
e.id_tipo_movimiento AS id_sub_canal,
f.id_tipo_movimiento AS id_tipo_movimiento,
g.id_tipo_movimiento AS id_producto,
a.tipo_movimiento_mes,
a.fecha_alta,
a.antiguedad_meses,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
a.nuevo_subcanal,
(CASE WHEN a.tipo_doc_cliente IS NULL OR a.tipo_doc_cliente='' THEN
(CASE WHEN LENGTH(a.identificacion_cliente)<10 THEN 'Pasaporte' 
WHEN LENGTH(a.identificacion_cliente)=10 THEN 'Cedula' 
WHEN LENGTH(a.identificacion_cliente)=13 THEN 'RUC' ELSE '' END)
ELSE a.tipo_doc_cliente END) AS tipo_doc_cliente,
a.p_fecha_factura
FROM db_desarrollo2021.tmp_costo_fact_exporta_otra_csts a
LEFT JOIN db_reportes.otc_t_catalogo_consolidado_id d
ON (UPPER(a.canal_comercial)=UPPER(d.tipo_movimiento)
AND d.extractor='Todos'
AND d.nombre_id='ID_CANAL')
LEFT JOIN db_reportes.otc_t_catalogo_consolidado_id e
ON (UPPER(a.nuevo_subcanal)=UPPER(e.tipo_movimiento)
AND e.extractor='Todos'
AND e.nombre_id='ID_SUBCANAL')
LEFT JOIN (SELECT (CASE WHEN tipo_movimiento='Renovación' THEN 'RENOVACION' ELSE UPPER(tipo_movimiento) END) AS tipo_movimiento,id_tipo_movimiento
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Terminales'
AND nombre_id='ID_TIPO_MOVIMIENTO') f
ON a.movimiento=f.tipo_movimiento
LEFT JOIN (SELECT (CASE WHEN tipo_movimiento='Prepago Crédito Directo' THEN 'Prepago Credito Directo'
WHEN tipo_movimiento='Equipo Libre Crédito Directo' THEN 'Equipo Libre Credito Directo'
WHEN tipo_movimiento='Contrato Crédito Directo' THEN 'Contrato Credito Directo'
ELSE tipo_movimiento END) AS tipo_movimiento,
id_tipo_movimiento
FROM db_reportes.otc_t_catalogo_consolidado_id
WHERE extractor='Terminales'
AND nombre_id='ID_PRODUCTO') g
ON (UPPER(CONCAT_WS(' ',a.movimiento,a.tipo_venta))=UPPER(g.tipo_movimiento));

--CREA TABLA TEMPORAL CON EL UNIVERSO PRINCIPAL SIN DUPLICADOS
CREATE TABLE db_desarrollo2021.tmp_fact_exporta_nodupli_csts AS
SELECT (CASE WHEN movimiento='PREPAGO' THEN 'PREPAGO'
WHEN a.movimiento IN('CONTRATO','RENOVACION') THEN 'POSPAGO' ELSE a.linea_negocio END) AS linea_negocio,
a.segmento,
a.sub_segmento,
a.movimiento,
a.fuente_movimiento,
a.telefono,
a.clasificacion,
a.num_factura,
a.num_factura_relacionada,
a.fecha_factura_relacionada,
a.fecha_factura,
a.usuario_factura,
a.nombre_usuario_factura,
a.usuario_final,
a.nombre_usuario_final,
a.oficina_usuario,
a.distribuidor_usuario,
a.codigo_tipo_documento,
a.tipo_documento,
a.oficina,
a.account_num,
a.nombre_cliente,
a.identificacion_cliente,
a.concepto_facturable,
a.modelo_terminal,
a.codigo_articulo,
a.descripcion_articulo,
a.imei,
a.tipo_cargo,
a.segmentacion_smarts,
a.fabricante,
a.modelo_guia_comercial,
a.gama_equipo,
a.clasificacion_terminal,
a.tecnologia,
a.codigo_da,
a.despacho,
a.razon_social,
a.canal_comercial,
a.tipo_canal,
a.fuente_canal,
a.region,
a.cantidad,
a.monto,
a.monto_unitario,
a.num_abonado,
a.plan_codigo,
a.plan_nombre,
a.tarifa_basica,
a.costo_unitario,
a.costo_total,
a.pvp_prepago,
a.subsidio_unitario,
a.fuente_costo,
a.fecha_proceso,
a.tienda,
a.branch,
a.canal_netcracker,
a.cuotas_financiadas,
a.tipo_venta,
a.ejecutivo_perimetro,
a.jefe_perimetro,
a.gerente_perimetro,
a.orden_venta,
a.fecha_creacion_orden_venta,
a.codigo_creador_orden_venta,
a.nombre_creador_orden_venta,
a.codigo_propietario_orden_venta,
a.nombre_propietario_orden_venta,
a.codigo_confirmador_orden_venta,
a.nombre_confirmador_orden_venta,
a.nota_credito_masiva,
a.precio_base,
a.precio_con_override,
a.usuario_override,
a.fecha_override,
a.cuota_inicial,
a.monto_financiado,
a.financiado_sin_iva,
a.forma_pago_factura,
a.tarjeta_banco,
a.cuotas,
a.tarjeta_banco2,
a.cuotas2,
a.tarjeta_banco3,
a.cuotas3,
a.segmento_final,
a.id_canal,
a.id_sub_canal,
a.id_tipo_movimiento,
a.id_producto,
b.tipo_movimiento_mes,
a.fecha_alta,
a.antiguedad_meses,
a.ruc_distribuidor,
a.codigo_plaza,
a.nom_plaza,
a.ciudad,
a.provincia,
(CASE WHEN a.canal_comercial='EMPRESAS GGCC' THEN 'EMPRESAS GGCC' 
WHEN a.canal_comercial='CAV PROPIO' THEN 'CAV PROPIO'
ELSE a.nuevo_subcanal END) AS nuevo_subcanal,
a.tipo_doc_cliente,
a.p_fecha_factura,
b.linea_negocio_homologado
FROM db_desarrollo2021.tmp_costo_fact_exporta_otra1_csts a
LEFT JOIN (SELECT num_telefonico,tipo_movimiento_mes,linea_negocio_homologado
FROM db_reportes.otc_t_360_general
WHERE fecha_proceso=${fecha_antes_ayer}
AND estado_abonado<>'BAA') b
ON a.telefono=b.num_telefonico;

--REALIZA EL BORRADO DE PARTICIONES CORRESPONDIENTES AL MES DE PROCESO (DESDE EL DIA 1 HASTA DIA CAIDO)
ALTER TABLE db_desarrollo2021.otc_t_terminales_simcards DROP PARTITION (p_fecha_factura>=${fecha_inicio},p_fecha_factura<${fecha_fin});

--REALIZA LA INSERCION DE INFORMACION DE TODAS LAS PARTICIONES CORRESPONDIENTES AL MES DE PROCESO (DESDE EL DIA 1 HASTA DIA CAIDO)
INSERT INTO db_desarrollo2021.otc_t_terminales_simcards PARTITION (p_fecha_factura)
SELECT linea_negocio,
segmento,
sub_segmento,
movimiento,
fuente_movimiento,
telefono,
clasificacion,
num_factura,
num_factura_relacionada,
fecha_factura_relacionada,
fecha_factura,
usuario_factura,
nombre_usuario_factura,
usuario_final,
nombre_usuario_final,
oficina_usuario,
distribuidor_usuario,
codigo_tipo_documento,
tipo_documento,
oficina,
account_num,
nombre_cliente,
identificacion_cliente,
concepto_facturable,
modelo_terminal,
codigo_articulo,
descripcion_articulo,
imei,
tipo_cargo,
segmentacion_smarts,
fabricante,
modelo_guia_comercial,
gama_equipo,
clasificacion_terminal,
tecnologia,
codigo_da,
despacho,
razon_social,
canal_comercial,
tipo_canal,
fuente_canal,
region,
cantidad,
monto,
monto_unitario,
num_abonado,
plan_codigo,
plan_nombre,
tarifa_basica,
costo_unitario,
costo_total,
pvp_prepago,
subsidio_unitario,
fuente_costo,
fecha_proceso,
tienda,
branch,
canal_netcracker,
cuotas_financiadas,
tipo_venta,
ejecutivo_perimetro,
jefe_perimetro,
gerente_perimetro,
orden_venta,
fecha_creacion_orden_venta,
codigo_creador_orden_venta,
nombre_creador_orden_venta,
codigo_propietario_orden_venta,
nombre_propietario_orden_venta,
codigo_confirmador_orden_venta,
nombre_confirmador_orden_venta,
nota_credito_masiva,
precio_base,
precio_con_override,
usuario_override,
fecha_override,
cuota_inicial,
monto_financiado,
financiado_sin_iva,
forma_pago_factura,
tarjeta_banco,
cuotas,
tarjeta_banco2,
cuotas2,
tarjeta_banco3,
cuotas3,
segmento_final,
id_canal,
id_sub_canal,
id_tipo_movimiento,
id_producto,
tipo_movimiento_mes,
fecha_alta,
antiguedad_meses,
UPPER(MD5(concat_ws('',
concat_ws('',
concat_ws('',
(CASE WHEN (telefono IS NULL OR telefono='') THEN '' ELSE telefono END),num_factura,account_num),
concat_ws('',CAST(concepto_facturable AS string),
(CASE WHEN (imei IS NULL OR imei='') THEN '' ELSE imei END),CAST(monto AS string)),
concat_ws('',
(CASE WHEN (cantidad IS NULL OR cantidad='') THEN '' ELSE cantidad END),
(CASE WHEN (cuotas IS NULL OR cuotas='') THEN '' ELSE cuotas END),
(CASE WHEN (num_abonado IS NULL OR num_abonado='') THEN '' ELSE num_abonado END))),
(CASE WHEN (codigo_articulo IS NULL OR codigo_articulo='') THEN '' ELSE codigo_articulo END),'${anio_mes}'))) AS id_hash,
ruc_distribuidor,
codigo_plaza,
nom_plaza,
ciudad,
provincia,
nuevo_subcanal,
tipo_doc_cliente,
linea_negocio_homologado,
p_fecha_factura
FROM db_desarrollo2021.tmp_fact_exporta_nodupli_csts;


--**************************************************************--
--******  Cambio de alcance (2023-02-01) por Cristian Ortiz ****--
--**************************************************************--

-- Tabla Temporal con terminales de notas de credito

CREATE TABLE db_desarrollo2021.tmp_terminales_simcards_nc AS
SELECT
		'${ultimo_dia_act_frmt}' AS fecha_proceso
		,(CASE
			WHEN fecha_factura IS NULL THEN '01/01/1990'
			ELSE 
			concat_ws('/'
			, SUBSTR(fecha_factura, 9, 2)
			, SUBSTR(fecha_factura, 6, 2)
			, SUBSTR(fecha_factura, 1, 4))
		END) AS fecha_factura
		, linea_negocio
		, segmento
		, sub_segmento
		, segmento_final
		, telefono
		, clasificacion
		,(CASE
			WHEN tipo_documento IS NULL THEN 'N/A'
			ELSE tipo_documento
		END) AS tipo_documento
		, num_factura
		, num_factura_relacionada
		,(CASE
			WHEN fecha_factura_relacionada IS NULL THEN '01/01/1990'
			ELSE
			concat_ws('/'
			, SUBSTR(fecha_factura_relacionada, 9, 2)
			, SUBSTR(fecha_factura_relacionada, 6, 2)
			, SUBSTR(fecha_factura_relacionada, 1, 4))
		END) AS fecha_factura_relacionada
		, oficina
		, account_num
		, nombre_cliente
		,UPPER(CASE
		WHEN (COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(tipo_doc_cliente, 'Ã¡', 'a'), 'Ã©', 'e'), 'Ã­', 'i'), 'Ã³', 'o'), 'Ãº', 'u'))) 
		IS NULL THEN 'N/A' 
		ELSE (COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(tipo_doc_cliente, 'Ã¡', 'a'), 'Ã©', 'e'), 'Ã­', 'i'), 'Ã³', 'o'), 'Ãº', 'u'))) END) AS tipo_doc_cliente
		, identificacion_cliente
		, modelo_terminal
		, imei
		, tipo_cargo
		, modelo_guia_comercial
		, clasificacion_terminal
		, cantidad
		, CAST(CAST(monto AS decimal(12, 2)) AS string) AS monto
		, num_abonado
		, movimiento
		,(CASE
			WHEN id_tipo_movimiento IS NULL THEN '-1'
			ELSE id_tipo_movimiento
		END) AS id_tipo_movimiento
		,(CASE
			WHEN id_producto IS NULL THEN '-1'
			ELSE id_producto
		END) AS id_producto
		, plan_codigo
		, plan_nombre
		,(CASE
			WHEN tarifa_basica IS NULL THEN ''
			ELSE CAST(CAST(tarifa_basica AS decimal(12, 2)) AS string)
		END) AS tarifa_basica
		, usuario_final
		, nombre_usuario_final
		, tipo_venta
		, cuotas_financiadas
		, ejecutivo_perimetro
		, jefe_perimetro
		, gerente_perimetro
		, nota_credito_masiva
		,UPPER(COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(forma_pago_factura, 'Ã¡', 'a'), 'Ã©', 'e'), 'Ã­', 'i'), 'Ã³', 'o'), 'Ãº', 'u'))) AS forma_pago_factura
		,(CASE
			WHEN cuota_inicial IS NULL THEN '-1'
			ELSE CAST(CAST(cuota_inicial AS decimal(12, 2)) AS string)
		END) AS cuota_inicial
		, canal_comercial
		,(CASE
			WHEN id_canal IS NULL THEN '-1'
			ELSE id_canal
		END) AS id_canal
		, oficina_usuario AS nom_distribuidor
		, ruc_distribuidor
		, codigo_plaza
		, nom_plaza
		, ciudad
		, provincia
		, region
		, nuevo_subcanal
		,(CASE
			WHEN id_sub_canal IS NULL THEN '-1'
			ELSE id_sub_canal
		END) AS id_sub_canal
		, tipo_movimiento_mes
		,(CASE
			WHEN fecha_alta IS NULL THEN '01/01/1990'
			ELSE concat_ws('/'
			, SUBSTR(date_format(fecha_alta, 'yyyy-MM-dd'), 9, 2)
			, SUBSTR(date_format(fecha_alta, 'yyyy-MM-dd'), 6, 2)
			, SUBSTR(date_format(fecha_alta, 'yyyy-MM-dd'), 1, 4))
		END) AS fecha_alta
		,(CASE
			WHEN CAST(CAST(antiguedad_meses AS decimal(12, 2)) AS string)<0 THEN '-1'
			ELSE CAST(CAST(antiguedad_meses AS decimal(12, 2)) AS string)
		END) AS antiguedad_meses
		, linea_negocio_homologado
		, id_hash
		, (CASE WHEN tipo_documento = 'NOTA DE CREDITO'
            THEN 'NO' END) AS aplica_comision
	FROM
        db_desarrollo2021.otc_t_terminales_simcards
	WHERE
		p_fecha_factura >= ${fecha_inicio}
		AND p_fecha_factura < ${fecha_fin}
		AND clasificacion IN ('ACCESORIOS', 'TERMINALES')
		AND tipo_cargo = 'CARGO'
		AND tipo_documento = 'NOTA DE CREDITO'
		AND concat_ws('', SUBSTR(fecha_factura_relacionada, 1, 4)
		, SUBSTR(fecha_factura_relacionada, 6, 2)
		, SUBSTR(fecha_factura_relacionada, 9, 2))>= '${fecha_inicio}'
		AND concat_ws('', SUBSTR(fecha_factura_relacionada, 1, 4)
		, SUBSTR(fecha_factura_relacionada, 6, 2)
		, SUBSTR(fecha_factura_relacionada, 9, 2))<'${fecha_fin}';

-- Tabla Temporal de terminales con factura, es decir diferentes de notas de credito

CREATE TABLE db_desarrollo2021.tmp_terminales_simcards_factura AS
SELECT
		'${ultimo_dia_act_frmt}' AS fecha_proceso
		,(CASE
			WHEN a.fecha_factura IS NULL THEN '01/01/1990'
			ELSE 
			concat_ws('/'
			, SUBSTR(a.fecha_factura, 9, 2)
			, SUBSTR(a.fecha_factura, 6, 2)
			, SUBSTR(a.fecha_factura, 1, 4))
		END) AS fecha_factura
		, a.linea_negocio
		, a.segmento
		, a.sub_segmento
		, a.segmento_final
		, a.telefono
		, a.clasificacion
		,(CASE
			WHEN a.tipo_documento IS NULL THEN 'N/A'
			ELSE a.tipo_documento
		END) AS tipo_documento
		, a.num_factura
		, a.num_factura_relacionada
		,(CASE
			WHEN a.fecha_factura_relacionada IS NULL THEN '01/01/1990'
			ELSE
			concat_ws('/'
			, SUBSTR(a.fecha_factura_relacionada, 9, 2)
			, SUBSTR(a.fecha_factura_relacionada, 6, 2)
			, SUBSTR(a.fecha_factura_relacionada, 1, 4))
		END) AS fecha_factura_relacionada
		, a.oficina
		, a.account_num
		, a.nombre_cliente
		, UPPER(CASE WHEN 
(COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.tipo_doc_cliente, 'Ã¡', 'a'), 'Ã©', 'e'), 'Ã­', 'i'), 'Ã³', 'o'), 'Ãº', 'u'))) 
IS NULL THEN 'N/A' ELSE (COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.tipo_doc_cliente, 'Ã¡', 'a'), 'Ã©', 'e'), 'Ã­', 'i'), 'Ã³', 'o'), 'Ãº', 'u'))) 
END) AS tipo_doc_cliente
		, a.identificacion_cliente
		, a.modelo_terminal
		, a.imei
		, a.tipo_cargo
		, a.modelo_guia_comercial
		, a.clasificacion_terminal
		, a.cantidad
		,CAST(CAST(a.monto AS decimal(12, 2)) AS string) AS monto
		, a.num_abonado
		, a.movimiento
		,(CASE
			WHEN a.id_tipo_movimiento IS NULL THEN '-1'
			ELSE a.id_tipo_movimiento
		END) AS id_tipo_movimiento
		,(CASE
			WHEN a.id_producto IS NULL THEN '-1'
			ELSE a.id_producto
		END) AS id_producto
		, a.plan_codigo
		, a.plan_nombre
		,(CASE
			WHEN a.tarifa_basica IS NULL THEN ''
			ELSE CAST(CAST(a.tarifa_basica AS decimal(12, 2)) AS string)
		END) AS tarifa_basica
		, a.usuario_final
		, a.nombre_usuario_final
		, a.tipo_venta
		, a.cuotas_financiadas
		, a.ejecutivo_perimetro
		, a.jefe_perimetro
		, a.gerente_perimetro
		, a.nota_credito_masiva
		,UPPER(COALESCE(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.forma_pago_factura, 'Ã¡', 'a'), 'Ã©', 'e'), 'Ã­', 'i'), 'Ã³', 'o'), 'Ãº', 'u'))) AS forma_pago_factura
		,(CASE
			WHEN a.cuota_inicial IS NULL THEN '-1'
			ELSE CAST(CAST(a.cuota_inicial AS decimal(12, 2)) AS string)
		END) AS cuota_inicial
		, a.canal_comercial
		,(CASE
			WHEN a.id_canal IS NULL THEN '-1'
			ELSE a.id_canal
		END) AS id_canal
		, a.oficina_usuario AS nom_distribuidor
		, a.ruc_distribuidor
		, a.codigo_plaza
		, a.nom_plaza
		, a.ciudad
		, a.provincia
		, a.region
		, a.nuevo_subcanal
		,(CASE
			WHEN a.id_sub_canal IS NULL THEN '-1'
			ELSE a.id_sub_canal
		END) AS id_sub_canal
		, a.tipo_movimiento_mes
		,(CASE
			WHEN a.fecha_alta IS NULL THEN '01/01/1990'
			ELSE concat_ws('/'
			, SUBSTR(date_format(a.fecha_alta, 'yyyy-MM-dd'), 9, 2)
			, SUBSTR(date_format(a.fecha_alta, 'yyyy-MM-dd'), 6, 2)
			, SUBSTR(date_format(a.fecha_alta, 'yyyy-MM-dd'), 1, 4))
		END) AS fecha_alta
		,(CASE
			WHEN CAST(CAST(a.antiguedad_meses AS decimal(12, 2)) AS string)<0 THEN '-1'
			ELSE CAST(CAST(a.antiguedad_meses AS decimal(12, 2)) AS string)
		END) AS antiguedad_meses
		, a.linea_negocio_homologado
		, a.id_hash
        , (CASE WHEN a.num_factura = b.num_factura_relacionada
        THEN 'NO' ELSE 'SI' END) AS aplica_comision
	FROM
		db_desarrollo2021.otc_t_terminales_simcards a
    LEFT JOIN db_desarrollo2021.tmp_terminales_simcards_nc b
    ON (a.num_factura = b.num_factura_relacionada)
	WHERE
		a.p_fecha_factura >= ${fecha_inicio}
		AND a.p_fecha_factura<${fecha_fin}
		AND a.clasificacion IN ('ACCESORIOS', 'TERMINALES')
		AND a.tipo_cargo = 'CARGO'
		AND a.tipo_documento <> 'NOTA DE CREDITO';

--- TABLA FINAL PARA REPORTE DE EXTRACTOR DE TERMINALES

CREATE TABLE db_desarrollo2021.tmp_terminales_simcards  AS
SELECT
	a.fecha_proceso AS fecha_proceso
	, a.fecha_factura AS fecha_factura
	, a.linea_negocio AS linea_negocio
	, a.segmento AS segmento
	, a.sub_segmento AS sub_segmento
	, a.segmento_final AS segmento_final
	, a.telefono AS telefono
	, a.clasificacion AS clasificacion
	, a.tipo_documento AS tipo_documento
	, a.num_factura AS num_factura
	, a.num_factura_relacionada AS num_factura_relacionada
	, a.fecha_factura_relacionada AS fecha_factura_relacionada
	, a.oficina AS oficina
	, a.account_num AS account_num
	, a.nombre_cliente AS nombre_cliente
	, a.tipo_doc_cliente AS tipo_doc_cliente
	, a.identificacion_cliente AS identificacion_cliente
	, a.modelo_terminal AS modelo_terminal
	, a.imei AS imei
	, a.tipo_cargo AS tipo_cargo
	, a.modelo_guia_comercial AS modelo_guia_comercial
	, a.clasificacion_terminal AS clasificacion_terminal
	, a.cantidad AS cantidad
	, a.monto AS monto
	, a.num_abonado AS num_abonado
	, a.movimiento AS movimiento
	, a.id_tipo_movimiento AS id_tipo_movimiento
	, a.id_producto AS id_producto
	, a.plan_codigo AS plan_codigo
	, a.plan_nombre AS plan_nombre
	, a.tarifa_basica AS tarifa_basica
	, a.usuario_final AS usuario_final
	, a.nombre_usuario_final AS nombre_usuario_final
	, a.tipo_venta AS tipo_venta
	, a.cuotas_financiadas AS cuotas_financiadas
	, a.ejecutivo_perimetro AS ejecutivo_perimetro
	, a.jefe_perimetro AS jefe_perimetro
	, a.gerente_perimetro AS gerente_perimetro
	, a.nota_credito_masiva AS nota_credito_masiva
	, a.forma_pago_factura AS forma_pago_factura
	, a.cuota_inicial AS cuota_inicial
	, a.canal_comercial AS canal_comercial
	, a.id_canal AS id_canal
	, a.nom_distribuidor AS nom_distribuidor
	, a.ruc_distribuidor AS ruc_distribuidor
	, a.codigo_plaza AS codigo_plaza
	, a.nom_plaza AS nom_plaza
	, a.ciudad AS ciudad
	, a.provincia AS provincia
	, a.region AS region
	, a.nuevo_subcanal AS nuevo_subcanal
	, a.id_sub_canal AS id_sub_canal
	, a.tipo_movimiento_mes AS tipo_movimiento_mes
	, a.fecha_alta AS fecha_alta
	, a.antiguedad_meses AS antiguedad_meses
	, a.linea_negocio_homologado AS linea_negocio_homologado
	, a.id_hash AS id_hash
    , a.aplica_comision as aplica_comision
FROM
	(SELECT 
    fecha_proceso
	,  fecha_factura
	,  linea_negocio
	,  segmento
	,  sub_segmento
	,  segmento_final
	,  telefono
	,  clasificacion
	,  tipo_documento
	,  num_factura
	,  num_factura_relacionada
	,  fecha_factura_relacionada
	,  oficina
	,  account_num
	,  nombre_cliente
	,  tipo_doc_cliente
	,  identificacion_cliente
	,  modelo_terminal
	,  imei
	,  tipo_cargo
	,  modelo_guia_comercial
	,  clasificacion_terminal
	,  cantidad
	,  monto
	,  num_abonado
	,  movimiento
	,  id_tipo_movimiento
	,  id_producto
	,  plan_codigo
	,  plan_nombre
	,  tarifa_basica
	,  usuario_final
	,  nombre_usuario_final
	,  tipo_venta
	,  cuotas_financiadas
	,  ejecutivo_perimetro
	,  jefe_perimetro
	,  gerente_perimetro
	,  nota_credito_masiva
	,  forma_pago_factura
	,  cuota_inicial
	,  canal_comercial
	,  id_canal
	,  nom_distribuidor
	,  ruc_distribuidor
	,  codigo_plaza
	,  nom_plaza
	,  ciudad
	,  provincia
	,  region
	,  nuevo_subcanal
	,  id_sub_canal
	,  tipo_movimiento_mes
	,  fecha_alta
	,  antiguedad_meses
	,  linea_negocio_homologado
	,  id_hash
    ,  aplica_comision
    FROM db_desarrollo2021.tmp_terminales_simcards_nc
UNION ALL
	SELECT 
    fecha_proceso
	,  fecha_factura
	,  linea_negocio
	,  segmento
	,  sub_segmento
	,  segmento_final
	,  telefono
	,  clasificacion
	,  tipo_documento
	,  num_factura
	,  num_factura_relacionada
	,  fecha_factura_relacionada
	,  oficina
	,  account_num
	,  nombre_cliente
	,  tipo_doc_cliente
	,  identificacion_cliente
	,  modelo_terminal
	,  imei
	,  tipo_cargo
	,  modelo_guia_comercial
	,  clasificacion_terminal
	,  cantidad
	,  monto
	,  num_abonado
	,  movimiento
	,  id_tipo_movimiento
	,  id_producto
	,  plan_codigo
	,  plan_nombre
	,  tarifa_basica
	,  usuario_final
	,  nombre_usuario_final
	,  tipo_venta
	,  cuotas_financiadas
	,  ejecutivo_perimetro
	,  jefe_perimetro
	,  gerente_perimetro
	,  nota_credito_masiva
	,  forma_pago_factura
	,  cuota_inicial
	,  canal_comercial
	,  id_canal
	,  nom_distribuidor
	,  ruc_distribuidor
	,  codigo_plaza
	,  nom_plaza
	,  ciudad
	,  provincia
	,  region
	,  nuevo_subcanal
	,  id_sub_canal
	,  tipo_movimiento_mes
	,  fecha_alta
	,  antiguedad_meses
	,  linea_negocio_homologado
	,  id_hash
    ,  aplica_comision
    FROM db_desarrollo2021.tmp_terminales_simcards_factura) a;


