with open(archivo_tab, "r") as archivo:
    lector = csv.reader(archivo, delimiter="\t")  # Indica que el separador es una tabulación
    for fila in lector:
        print(fila)  # Esto imprimirá cada fila del archivo



dataframe = pd.read_csv(archivo_tab, delimiter="\t", dtype=str)
dataframe = dataframe.astype(str)
spark_df = spark.createDataFrame(dataframe)
spark_df.write.format('hive').format("parquet").mode("overwrite").saveAsTable('db_desarrollo2021.factura_contado_20230821')

archivo_tab = "/home/nae108834/cp_terminales_simcards/Facturas_miscelaneas_afecta_cuota.txt"
dataframe = pd.read_csv(archivo_tab, delimiter="\t", dtype=str)
dataframe = dataframe.astype(str)
spark_df = spark.createDataFrame(dataframe)
spark_df.write.format('hive').format("parquet").mode("overwrite").saveAsTable('db_desarrollo2021.f_m_afecta_20230821')

archivo_tab = "/home/nae108834/cp_terminales_simcards/Notas_credito.txt"
dataframe = pd.read_csv(archivo_tab, delimiter="\t", dtype=str)
dataframe = dataframe.astype(str)
spark_df = spark.createDataFrame(dataframe)
spark_df.write.format('hive').format("parquet").mode("overwrite").saveAsTable('db_desarrollo2021.notas_credito_20230821')




