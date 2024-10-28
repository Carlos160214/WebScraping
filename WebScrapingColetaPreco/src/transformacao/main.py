

import pandas as pd
import oracledb
from datetime import datetime

# Configuração do TNS (alterar conforme necessário)
tns_name = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=gb90e693e60eae1_adwxpe_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"

# Credenciais do banco de dados
username = 'ADMIN'
password = '123456789Deus@'

# Ler o arquivo CSV
df = pd.read_json(r"/opt/airflow/dags/script/WebScrapingColetaPreco/data/data1.jsonl", lines=True, dtype='string')

# Configurações do pandas
pd.options.display.max_columns = None

# Adicionar coluna com data da coleta
df['data_coleta'] = datetime.now()
df['data_formatada'] = pd.to_datetime(df['data_coleta']).dt.strftime('%d/%m/%Y')

# Transformações de dados - new
df['preco_new'] = df['preco_new'].str.replace('[^0-9]', '', regex=True).astype(float)
df['cent_new']  = df['cent_new'].str.replace('[^0-9]', '', regex=True).astype(float)
df['cent_new']  = df['cent_new'].fillna(0)
df['preco_new_total'] = df['preco_new'] + df['cent_new'] / 100


# # Transformações de dados
# df['preco_new'] = df['preco_new'].str.replace('[^0-9]', '', regex=True).astype(float)
# df['cent_new']  = df['preco_new'].str.replace('[^0-9]', '', regex=True).astype(float)

# #Calcular preço total
# df['preco_new_total'] = df['preco_new'] + df['cent_new'] / 100

#Primeira letra de cada string em maiúscula e as demais em minúsculas.
df['categoria']       = df['categoria'].str.capitalize()
df['dsc_produto']     = df['dsc_produto'].str.capitalize()

# Função para verificar se um item existe na tabela DIM_ITEM
def check_exists(cursor, dsc_item):
    query = "SELECT COUNT(*) FROM DIM_ITEM WHERE UPPER(DSC_ITEM) = UPPER(:DSC_ITEM)"
    cursor.execute(query, {'DSC_ITEM': dsc_item})
    count, = cursor.fetchone()
    return count > 0

# Função para buscar o COD_ITEM na tabela DIM_ITEM
def get_cod_item(cursor, dsc_item):
    query = "SELECT COD_ITEM FROM DIM_ITEM WHERE UPPER(DSC_ITEM) = UPPER(:DSC_ITEM)"
    cursor.execute(query, {'DSC_ITEM': dsc_item})
    result = cursor.fetchone()
    return result[0] if result else None

try:
    # Cria a conexão com o banco de dados
    connection = oracledb.connect(user=username, password=password, dsn=tns_name)
    print("Conexão estabelecida com sucesso!")

    with connection.cursor() as cursor:
        for index, row in df.iterrows():
            dsc_item = row['dsc_produto']
            if not check_exists(cursor, dsc_item):
                # Inserção na tabela DIM_ITEM
                query = """
                INSERT INTO DIM_ITEM (COD_ITEM, DSC_ITEM, DSC_CATEGORIA, FONTE, LINK) 
                VALUES (SEQ_DIM_ITEM.NEXTVAL, :DSC_ITEM, :DSC_CATEGORIA, :FONTE, :LINK)
                """
                cursor.execute(query, {
                    'DSC_ITEM': row['dsc_produto'],
                    'DSC_CATEGORIA': row['categoria'],
                    'LINK': row['link'],
                    'FONTE' : row['fonte']
                })
                connection.commit()
                print("Item inserido na DIM_ITEM com sucesso!")

            cod_item = get_cod_item(cursor, dsc_item)

            if cod_item:
                    # Inserção na tabela FATO_COLETA_PRECO
                query = """INSERT INTO FATO_COLETA_PRECO (DT_DATA, COD_ITEM, DT_HR_INTEGRACAO, PRECO) 
                           VALUES (TO_DATE(:DT_DATA, 'dd/mm/yyyy'), :COD_ITEM, :DT_HR_INTEGRACAO, :PRECO)"""
                cursor.execute(query, {
                        'DT_DATA': row['data_formatada'],
                        'COD_ITEM': cod_item,
                        'DT_HR_INTEGRACAO': row['data_coleta'],
                        'PRECO': row['preco_new_total']
                    })
                connection.commit()
                print(f"Dados inseridos na FATO_COLETA_PRECO para o item {cod_item} com sucesso!")

except oracledb.Error as error:
    print('Erro ao inserir no banco:')
    print(error)

finally:
    # Fecha a conexão
    if 'connection' in locals():
        connection.close()
        print("Conexão fechada.")







