# -- coding: utf-8 --
import sys
reload(sys)
from functools import wraps
import time
from datetime import datetime
import os
from pyspark.sql.functions import col, substring_index

#db_desarrollo2021.otc_t_catalogo_terminales -> db_desarrollo2021.otc_t_catalogo_terminales_prycldr 
#db_desarrollo2021.otc_t_terminales_fact -> db_desarrollo2021.otc_t_terminales_fact_prycldr
#db_desarrollo2021.otc_t_terminales_nc -> db_desarrollo2021.otc_t_terminales_nc_prycldr
#db_rdb.otc_t_r_cbm_bill -> db_desarrollo2021.otc_t_r_cbm_bill_prycldr
#db_rdb.otc_t_r_am_cpe -> db_desarrollo2021.otc_t_r_am_cpe_prycldr
#db_rdb.otc_t_v_usuarios -> db_desarrollo2021.otc_t_v_usuarios_prycldr
#db_cs_terminales.otc_t_catalogo_ruc_das_retail -> db_desarrollo2021.otc_t_catalogo_ruc_das_retail_prycldr
#db_cs_terminales.otc_t_catalogo_tipo_canal ->  db_desarrollo2021.otc_t_catalogo_tipo_canal_prycldr
#db_cs_terminales.otc_t_catalogo_canal_online ->  db_desarrollo2021.otc_t_catalogo_canal_online_prycldr
#db_cs_terminales.otc_t_asigna_canal_ventas -> db_desarrollo2021.otc_t_asigna_canal_ventas_prycldr
#db_cs_terminales.otc_t_ctl_cat_seg_sub_seg -> db_desarrollo2021.otc_t_ctl_cat_seg_sub_seg_prycldr
#db_cs_terminales.otc_t_ctl_seg_terminal -> db_desarrollo2021.otc_t_ctl_seg_terminal_prycldr

##**************************************************************--
##******  Cambio de alcance (2023-04-12)  (Cristian Ortiz) ****--
##**************************************************************--
## 1
## Tabla Temporal con terminales de notas de credito
def tmp_terminales_simcards_nc(val_fecha_formato,val_fecha_ini,val_dia_uno):
    qry="""
SELECT
		'{val_fecha_formato}' AS fecha_proceso
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
		p_fecha_factura >= {val_fecha_ini}
		AND p_fecha_factura < {val_dia_uno}
		AND clasificacion IN ('ACCESORIOS', 'TERMINALES')
		AND tipo_cargo = 'CARGO'
		AND tipo_documento = 'NOTA DE CREDITO'
		AND concat_ws('', SUBSTR(fecha_factura_relacionada, 1, 4)
		, SUBSTR(fecha_factura_relacionada, 6, 2)
		, SUBSTR(fecha_factura_relacionada, 9, 2))>= '{val_fecha_ini}'
		AND concat_ws('', SUBSTR(fecha_factura_relacionada, 1, 4)
		, SUBSTR(fecha_factura_relacionada, 6, 2)
		, SUBSTR(fecha_factura_relacionada, 9, 2))<'{val_dia_uno}'
    """.format(val_fecha_formato=val_fecha_formato,val_fecha_ini=val_fecha_ini,val_dia_uno=val_dia_uno)
    print(qry)
    return qry  
##2
## Tabla Temporal de terminales con factura, es decir diferentes de notas de credito
def tmp_terminales_simcards_factura(val_fecha_formato,val_fecha_ini,val_dia_uno):
    qry="""
SELECT
		'{val_fecha_formato}' AS fecha_proceso
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
    LEFT JOIN tmp_terminales_simcards_nc b
    ON (a.telefono = b.telefono)
    AND (a.account_num = b.account_num)
    AND (a.num_factura = b.num_factura_relacionada)
	WHERE
		a.p_fecha_factura >= {val_fecha_ini}
		AND a.p_fecha_factura<{val_dia_uno}
		AND a.clasificacion IN ('ACCESORIOS', 'TERMINALES')
		AND a.tipo_cargo = 'CARGO'
		AND a.tipo_documento <> 'NOTA DE CREDITO'
    """.format(val_fecha_formato=val_fecha_formato,val_fecha_ini=val_fecha_ini,val_dia_uno=val_dia_uno)
    print(qry)
    return qry  
##3
## TABLA con la union de terminales NC y facturas
def tmp_terminales_simcards(fecha_antes_ayer):
    qry="""
SELECT
	a.fecha_proceso AS fecha_proceso
	, a.fecha_factura AS fecha_factura
	, a.linea_negocio AS linea_negocio
	, a.segmento AS segmento
    , (CASE WHEN A.sub_segmento LIKE 'PEQUE%' THEN 'PEQUEÃ‘AS' ELSE A.sub_segmento END) AS sub_segmento
	--, a.sub_segmento AS sub_segmento
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
	, concat_ws('/'
			, SUBSTR(date_format(g.fecha_alta, 'yyyy-MM-dd'), 9, 2)
			, SUBSTR(date_format(g.fecha_alta, 'yyyy-MM-dd'), 6, 2)
			, SUBSTR(date_format(g.fecha_alta, 'yyyy-MM-dd'), 1, 4)) AS fecha_alta
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
    FROM tmp_terminales_simcards_nc
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
    FROM tmp_terminales_simcards_factura) a
	LEFT JOIN db_reportes.otc_t_360_general g 
	ON
        (a.telefono=g.num_telefonico)
	AND
        (a.account_num=g.account_num)
    AND g.fecha_proceso={fecha_antes_ayer}
    """.format(fecha_antes_ayer=fecha_antes_ayer)
    print(qry)
    return qry  
#4 TABLA FINAL PARA REPORTE DE EXTRACTOR DE TERMINALES
def otc_t_ext_terminales_ajst():
    qry="""
SELECT
	tsim.fecha_proceso AS fecha_proceso
	,  tsim.fecha_factura
	,  tsim.linea_negocio
	,  tsim.segmento
	,  tsim.sub_segmento
	,  tsim.segmento_final
	,  tsim.telefono
	,  tsim.clasificacion
	,  tsim.tipo_documento
	,  tsim.num_factura
	,  tsim.num_factura_relacionada
	,  tsim.fecha_factura_relacionada
	,  tsim.oficina
	,  tsim.account_num
	,  tsim.nombre_cliente
	,  tsim.tipo_doc_cliente
	,  tsim.identificacion_cliente
	,  tsim.modelo_terminal
	,  tsim.imei
	,  tsim.tipo_cargo
	,  tsim.modelo_guia_comercial
	,  tsim.clasificacion_terminal
	,  tsim.cantidad
	,  tsim.monto
	,  tsim.num_abonado
	,  tsim.movimiento
	,  tsim.id_tipo_movimiento
	,  tsim.id_producto
	,  tsim.plan_codigo
	,  tsim.plan_nombre
	,  tsim.tarifa_basica
	,  tsim.usuario_final AS usuario_final
	,  tsim.nombre_usuario_final AS nombre_usuario_final
	,  tsim.tipo_venta
	,  tsim.cuotas_financiadas
	,  tsim.ejecutivo_perimetro
	,  tsim.jefe_perimetro
	,  tsim.gerente_perimetro
	,  tsim.nota_credito_masiva
	,  tsim.forma_pago_factura
	,  tsim.cuota_inicial
	,  tsim.canal_comercial AS canal_comercial
	,  tsim.id_canal AS id_canal
	,  tsim.nom_distribuidor  AS nom_distribuidor
	,  tsim.ruc_distribuidor AS ruc_distribuidor
	,  tsim.codigo_plaza
	,  tsim.nom_plaza
	,  tsim.ciudad
	,  tsim.provincia
	,  tsim.region
	,  tsim.nuevo_subcanal AS nuevo_subcanal
	,  tsim.id_sub_canal AS id_sub_canal
	,  tsim.tipo_movimiento_mes
	,  tsim.fecha_alta
	,  tsim.antiguedad_meses
	,  tsim.linea_negocio_homologado
	,  tsim.id_hash
    ,  tsim.aplica_comision 
FROM
	tmp_terminales_simcards tsim
UNION ALL
SELECT
	t.fecha_proceso AS fecha_proceso
	,  t.fecha_factura
	,  t.linea_negocio
	,  t.segmento
	,  t.sub_segmento
	,  t.segmento_final
	,  t.telefono
	,  t.clasificacion
	,  t.tipo_documento
	,  t.num_factura
	,  t.num_factura_relacionada
	,  t.fecha_factura_relacionada
	,  t.oficina
	,  t.account_num
	,  t.nombre_cliente
	,  t.tipo_doc_cliente
	,  t.identificacion_cliente
	,  t.modelo_terminal
	,  t.imei
	,  t.tipo_cargo
	,  t.modelo_guia_comercial
	,  t.clasificacion_terminal
	,  t.cantidad
	,  t.monto
	,  t.num_abonado
	,  t.movimiento
	,  t.id_tipo_movimiento
	,  t.id_producto
	,  t.plan_codigo
	,  t.plan_nombre
	,  t.tarifa_basica
	,  nvl(ajt.usuario_final,t.usuario_final) AS usuario_final
	,  nvl(ajt.nombre_usuario_final,t.nombre_usuario_final) AS nombre_usuario_final
	,  t.tipo_venta
	,  t.cuotas_financiadas
	,  t.ejecutivo_perimetro
	,  t.jefe_perimetro
	,  t.gerente_perimetro
	,  t.nota_credito_masiva
	,  t.forma_pago_factura
	,  t.cuota_inicial
	,  nvl(ajt.canal_comercial,t.canal_comercial) AS canal_comercial
	,  nvl(ajt.id_canal,t.id_canal) AS id_canal
	,  nvl(ajt.nom_distribuidor,t.nom_distribuidor)  AS nom_distribuidor
	,  nvl(ajt.ruc_distribuidor,t.ruc_distribuidor) AS ruc_distribuidor
	,  t.codigo_plaza
	,  t.nom_plaza
	,  t.ciudad
	,  t.provincia
	,  t.region
	,  nvl(ajt.nuevo_subcanal,t.nuevo_subcanal) AS nuevo_subcanal
	,  nvl(ajt.id_sub_canal,t.id_sub_canal) AS id_sub_canal
	,  t.tipo_movimiento_mes
	,  t.fecha_alta
	,  t.antiguedad_meses
	,  t.linea_negocio_homologado
	,  UPPER(MD5(concat_ws('',t.id_hash,ajt.ruc_distribuidor))) as id_hash
    ,  t.aplica_comision 
FROM
	tmp_terminales_simcards t
INNER JOIN 
    db_desarrollo2021.otc_t_ajsts_terminales ajt
ON  
    (ajt.fecha_proceso=t.fecha_proceso)
AND 
    (ajt.num_factura=t.num_factura)
AND 
    (ajt.imei=t.imei)
    """
    print(qry)
    return qry

def sql_file():
    qry="""
SELECT DISTINCT 
fecha_proceso
, fecha_factura
, linea_negocio
, segmento
, sub_segmento
, segmento_final
, telefono
, clasificacion
, tipo_documento
, num_factura
, num_factura_relacionada
, fecha_factura_relacionada
, oficina
, account_num
, nombre_cliente
, tipo_doc_cliente
, identificacion_cliente
, modelo_terminal
, imei
, tipo_cargo
, modelo_guia_comercial
, clasificacion_terminal
, cantidad
, monto
, num_abonado
, movimiento
, id_tipo_movimiento
, id_producto
, plan_codigo
, plan_nombre
, tarifa_basica
, usuario_final
, nombre_usuario_final
, tipo_venta
, cuotas_financiadas
, ejecutivo_perimetro
, jefe_perimetro
, gerente_perimetro
, nota_credito_masiva
, forma_pago_factura
, cuota_inicial
, canal_comercial
, id_canal
, nom_distribuidor
, ruc_distribuidor
, codigo_plaza
, nom_plaza
, ciudad
, provincia
, region
, nuevo_subcanal
, id_sub_canal
, tipo_movimiento_mes
, fecha_alta
, antiguedad_meses
, linea_negocio_homologado
, id_hash
, aplica_comision
FROM db_desarrollo2021.otc_t_ext_terminales_ajst
    """
    print(qry)
    return qry


