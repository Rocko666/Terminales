
CREATE TABLE db_desarrollo2021.temp_terminales_nc
(
  `office_code` string, 
  `office_name` string, 
  `usuario` string, 
  `account_num` string, 
  `num_abonado` string, 
  `bill_dtm` timestamp, 
  `document_type_id` decimal(2,0), 
  `documento` string, 
  `origin_doc_type_id` decimal(2,0), 
  `document_type_name` string, 
  `nro_nota_credito` string, 
  `origin_invoice_num` string, 
  `customer_id_type` string, 
  `customer_id_number` string, 
  `nombre_cliente` string, 
  `revenue_code_id` decimal(9,0), 
  `revenue_code_desc` string, 
  `imei_imsi` string, 
  `monto` decimal(38,10)
)
COMMENT 'Tabla particionada con la informacion de Oracle Notas de credito terminales'
PARTITIONED BY (pt_fecha bigint)
STORED AS PARQUET
TBLPROPERTIES('transactional'='false');



CREATE TABLE db_desarrollo2021.temp_terminales_fact
(
  `fecha_factura` timestamp, 
  `bill_status` decimal(2,0), 
  `sri_authorization_date` timestamp, 
  `document_type_id` decimal(2,0), 
  `document_type_name` string, 
  `invoice_num` string, 
  `office_code` string, 
  `office_name` string, 
  `usuario` string, 
  `account_num` string, 
  `num_abonado` string, 
  `nombre_cliente` string, 
  `customer_id_number` string, 
  `revenue_code_id` decimal(9,0), 
  `revenue_code_desc` string, 
  `imei_imsi` string, 
  `product_quantity` decimal(9,0), 
  `monto` decimal(38,10)
)
COMMENT 'Tabla particionada con la informacion de Oracle Facturacion terminales'
PARTITIONED BY (pt_fecha bigint)
STORED AS PARQUET
TBLPROPERTIES('transactional'='false');