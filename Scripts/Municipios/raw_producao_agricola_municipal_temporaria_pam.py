import functions as c
import os
import pandas as pd
import numpy as np
from datetime import datetime
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
raw_path = c.raw_path
pam_path = os.path.join(raw_path, 'producao_agricola_municipal_temporaria_pam')


# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

for year in range(2010, datetime.now().year + 1):
    if not os.path.exists(os.path.join(pam_path, f'raw_producao_agricola_municipal_temporaria_pam_{year}.parquet')):

        # baixa os dados caso o arquivo do ano ainda nao exista
        url = f'https://apisidra.ibge.gov.br/values/t/1612/n6/2801207,2802403,2804201,2804508,2805406,2805604,2800100,2800704,2801108,2802700,2804409,2804706,2804904,2805703,2807303,2800209,2801405,2801603,2804458,2801900,2802205,2802304,2802601,2803104,2803401,2803807,2804300,2804607,2805000,2805208,2806008,2806909,2807006,2801306,2801504,2802007,2802502,2803302,2803609,2804003,2805307,2805901,2806107,2806503,2806602,2807204,2800506,2801009,2802908,2803708,2803906,2804102,2806800,2800407,2800670,2803005,2803500,2805109,2805802,2806206,2807105,2800308,2800605,2802106,2802809,2803203,2804805,2806305,2806701,2807600,2801702,2805505,2807402,2807501,2806404/v/all/p/{year}/c81/allxt/d/v1000109%202,v1000215%202,v1000216%202'
        session = c.create_session_with_retries()
        response = session.get(url, timeout=session.request_timeout, headers=c.headers)
        data = pd.DataFrame(response.json())

        # faz um simples tratamento dos dados
        # usa a primeira linha como cabeçalho e a remove do dataframe
        # seleciona as colunas de interesse
        # altera o tipo das colunas
        data.columns = data.iloc[0]
        data = data.iloc[1:][['Unidade de Medida', 'Valor', 'Município', 'Variável', 'Ano', 'Produto das lavouras temporárias']]
        data['Ano'] = data['Ano'].astype('int16')
        data['Valor'] = pd.to_numeric(data['Valor'], errors='coerce')

        data.to_parquet(os.path.join(pam_path, f'raw_producao_agricola_municipal_temporaria_pam_{year}.parquet'), engine='pyarrow', compression='snappy', index=False)
        print(f'Base de dados do ano {year} baixada com sucesso!')

        # cria a base de dados unica se ainda nao existir
        if not os.path.exists(os.path.join(pam_path, f'raw_producao_agricola_municipal_temporaria_pam_unico.parquet')):
            print('Criando base de dados unica...')
            print(f'A base se inicia no ano {year}')
            data.to_parquet(os.path.join(pam_path, f'raw_producao_agricola_municipal_temporaria_pam_unico.parquet'), engine='pyarrow', compression='snappy', index=False)
        
        # se a base de dados unica ja existir, adiciona os dados do ano atual que ainda não foram adicionados ao arquivo
        else:
            temp_data = pd.read_parquet(os.path.join(pam_path, f'raw_producao_agricola_municipal_temporaria_pam_unico.parquet'))
            if not year in temp_data['Ano'].unique():
                print(f'Adicionando dados do ano {year} a base de dados unica...')
                temp_data = pd.concat([temp_data, data], ignore_index=True)
                temp_data.to_parquet(os.path.join(pam_path, f'raw_producao_agricola_municipal_temporaria_pam_unico.parquet'), engine='pyarrow', compression='snappy', index=False)
        
    # se a base de dados do ano atual ja existir, pula para o ano seguinte
    else:
        print(f'Base de dados do ano {year} já existe. Pulando para o próximo ano...')
        sleep(1)

