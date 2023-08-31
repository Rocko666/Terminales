
--AGREGA NUEVOS CAMPOS A TABLA DESTINO otc_t_terminales_simcards
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (forma_pago_factura varchar(40) COMMENT 'forma de pago para facturas');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (tarjeta_banco varchar(40) COMMENT 'tarjeta de banco');  
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (cuotas varchar(3) COMMENT 'cuotas del producto');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (tarjeta_banco2 varchar(40) COMMENT 'tarjeta de banco2');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (cuotas2 varchar(3) COMMENT 'cuotas2 del producto');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (tarjeta_banco3 varchar(40) COMMENT 'tarjeta de banco3');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (cuotas3 varchar(3) COMMENT 'cuotas3 del producto');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (segmento_final string COMMENT 'segmento final');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (id_canal string COMMENT 'identificador del canal');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (id_sub_canal string COMMENT 'identificador del sub canal');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (id_tipo_movimiento string COMMENT 'identificador del tipo de movimiento');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (id_producto string COMMENT 'identificador del producto');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (tipo_movimiento_mes string COMMENT 'tipo de movimiento en el mes');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (fecha_alta timestamp COMMENT 'fecha de alta');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (antiguedad_meses double COMMENT 'antiguedad en meses');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (id_hash string COMMENT 'id hash del registro conformado por el telefono, cuenta y fecha proceso');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (ruc_distribuidor string COMMENT 'identificador del distribuidor');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (codigo_plaza string COMMENT 'codigo de la plaza');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (nom_plaza string COMMENT 'nombre de la plaza');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (ciudad string COMMENT 'ciudad');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (provincia string COMMENT 'provincia');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (nuevo_subcanal string COMMENT 'nuevo sub canal');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (tipo_doc_cliente string COMMENT 'tipo de documento del cliente');
ALTER TABLE db_cs_terminales.otc_t_terminales_simcards ADD COLUMNS (linea_negocio_homologado string COMMENT 'linea de negocio homologada');

