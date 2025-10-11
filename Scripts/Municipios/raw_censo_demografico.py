import traceback
import functions as c
import os
import pandas as pd


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
db_name = 'censo_demografico'  # nome base da base de dados
file_name = 'raw_' + db_name + '.parquet'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************
try:
    print('Iniciando download dos dados do SIDRA...')

    # Lista de municípios
    municipios = '2801207,2802403,2804201,2804508,2805406,2805604,2800100,2800704,2801108,2802700,2804409,2804706,2804904,2805703,2807303,2800209,2801405,2801603,2804458,2801900,2802205,2802304,2802601,2803104,2803401,2803807,2804300,2804607,2805000,2805208,2806008,2806909,2807006,2801306,2801504,2802007,2802502,2803302,2803609,2804003,2805307,2805901,2806107,2806503,2806602,2807204,2800506,2801009,2802908,2803708,2803906,2804102,2806800,2800407,2800670,2803005,2803500,2805109,2805802,2806206,2807105,2800308,2800605,2802106,2802809,2803203,2804805,2806305,2806701,2807600,2801702,2805505,2807402,2807501,2806404'

    # Baixa os dados do censo demográfico
    url = f'https://apisidra.ibge.gov.br/values/t/4709/n6/{municipios}/v/93/p/all'

    print('Baixando dados do SIDRA...')
    session = c.create_session_with_retries()
    response = session.get(url, timeout=session.request_timeout, headers=c.headers)
    response.raise_for_status()

    data = pd.DataFrame(response.json())

    print(f'Total de dados coletados: {data.shape[0]} linhas, {data.shape[1]} colunas')

    # Tratamento dos dados
    print('Processando e limpando os dados...')

    # A primeira linha contém os nomes das colunas
    data.columns = data.iloc[0]
    data = data.iloc[1:][['Unidade de Medida', 'Valor', 'Município', 'Variável', 'Ano']]

    # Converte tipos de dados
    data['Ano'] = pd.to_numeric(data['Ano']).astype('Int16')
    data['Valor'] = pd.to_numeric(data['Valor'], errors='coerce').astype('float32')
    data['Município'] = data['Município'].astype('category')
    data['Variável'] = data['Variável'].astype('category')

    # Salva o arquivo
    print(f'Salvando dados em: {file_path}')
    data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Arquivo {file_name} salvo com sucesso!')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_censo_demografico.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_censo_demografico.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_censo_demografico.txt".')

