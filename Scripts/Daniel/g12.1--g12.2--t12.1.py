import functions as c
import os
import pandas as pd
import numpy as np
import json
import traceback
import tempfile
import shutil
import sidrapy
import ipeadatapy
from datetime import datetime
import time
import re
import requests
from io import BytesIO


# pandas config
pd.set_option('display.float_format', lambda x: '{:,.4f}'.format(x))  # mostra os números com 4 casas decimais e separador de milhar

# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# download do arquivo do banco central - bcb
try:
    # Sessão persistente para manter os cookies entre requisições
    session = requests.Session()

    # URL da consulta
    consulta_url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarValores"

    # Parâmetros do formulário
    year = str(datetime.now().year)
    form_data = {
        'optSelecionaSerie': ['13345', '13350', '13356', '13943', '13944', '22708', '22709'],
        'dataInicio': '01/01/2010',
        'dataFim': f'31/12/{year}',
        'selTipoArqDownload': '0',
        'chkPaginar': 'on',
        'hdOidSeriesSelecionadas': '13345;13350;13356;13943;13944;22708;22709',
        'hdPaginar': 'true',
        'bilServico': '[SGSFW2301]'
    }

    # Headers de navegador
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
    }

    # Envia a requisição de consulta
    consulta_response = session.post(consulta_url, headers=headers, data=form_data)

    # Agora vamos simular o download do CSV
    download_url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=downLoad"

    download_headers = {
        'Referer': consulta_url,
        'User-Agent': headers['User-Agent'],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }

    # Faz a requisição GET para baixar o CSV
    csv_response = session.get(download_url, headers=download_headers)
    
    # Tratamento dos dados
    df = pd.read_csv(BytesIO(csv_response.content), sep=";", encoding="latin1")
    
    # Tratamento dos dados
    df_final = df.loc[~(df[df.columns[0]] == 'Fonte')].copy()  # filtra apenas as linhas com datas válidas
    df_melted = df_final.melt(id_vars=['Data'], value_vars=df_final.columns[1:], var_name='Variável', value_name='Valor')  # transforma os dados em formato longo
    df_melted = df_melted.dropna(subset=['Valor'])  # remove linhas com valores NaN
    df_melted['Região'] = df_melted['Variável'].str.split(' - ').str[2]  # extrai o estado da variável
    df_melted['Variável'] = df_melted['Variável'].str.split(' - ').str[1]  # extrai a variável

    df_export = df_melted[['Data', 'Variável', 'Região', 'Valor']].copy()  # seleciona as colunas relevantes
    df_export['Data'] = pd.to_datetime(df_export['Data'], format='%m/%Y')  # converte a coluna de data para o formato datetime
    df_export['Valor'] = df_export['Valor'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)  # ajusta o formato dos números
    df_export['Valor'] = pd.to_numeric(df_export['Valor'], errors='coerce')  # converte a coluna de valor para float, tratando erros
    df_export.loc[~(df_export['Região'].isin(['Sergipe', 'Nordeste'])), 'Região'] = 'Brasil'  # unifica as regiões Brasil e Nordeste
    df_export['Variável'] = df_export['Variável'].str.capitalize()  # capitaliza a primeira letra de cada palavra na coluna Variável
    df_export['Região'] = df_export['Região'].str.capitalize()  # capitaliza a primeira letra de cada palavra na coluna Região

    c.to_excel(df_export, dbs_path, 'bcb.xlsx')

except Exception as e:
    errors['Banco Central - BCB'] = traceback.format_exc()


# import requests
# import pandas as pd
# from io import BytesIO
# from datetime import datetime
# import time

# # Lista completa de séries
# series_ids = [
#     '13345', '13350', '13356', '13561', '13562', '13563',
#     '13575', '13576', '13577', '13581', '13582', '13583',
#     '13943', '13944', '13945', '20179', '20180', '20181'
# ]

# # Parâmetros fixos
# start_date = '01/01/2010'
# end_date = f'31/12/{datetime.now().year}'
# max_series_per_request = 10

# # URL base
# consulta_url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarValores"
# download_url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=downLoad"

# # Headers de navegador
# headers = {
#     'Content-Type': 'application/x-www-form-urlencoded',
#     'Referer': 'https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint',
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
# }

# # DataFrame acumulador
# df = pd.DataFrame()

# # Loop em lotes de até 10 séries
# for i in range(0, len(series_ids), max_series_per_request):
#     batch = series_ids[i:i + max_series_per_request]
#     batch_str = ';'.join(batch)

#     form_data = {
#         'optSelecionaSerie': batch,
#         'dataInicio': start_date,
#         'dataFim': end_date,
#         'selTipoArqDownload': '0',
#         'chkPaginar': 'on',
#         'hdOidSeriesSelecionadas': batch_str,
#         'hdPaginar': 'true',
#         'bilServico': '[SGSFW2301]'
#     }

#     with requests.Session() as session:
#         # Consulta
#         session.post(consulta_url, headers=headers, data=form_data)
        
#         # Download do CSV
#         download_headers = {
#             'Referer': consulta_url,
#             'User-Agent': headers['User-Agent'],
#             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#         }
#         csv_response = session.get(download_url, headers=download_headers)

#         # Leitura e concatenação
#         df_batch = pd.read_csv(BytesIO(csv_response.content), sep=";", encoding="latin1")
#         df = pd.concat([df, df_batch], ignore_index=True)

#     # Pequeno intervalo entre requisições para evitar bloqueios
#     time.sleep(1)

# # Tratamento dos dados
# df_final = df.loc[~(df[df.columns[0]] == 'Fonte')].copy()  # filtra apenas as linhas com datas válidas
# df_melted = df_final.melt(id_vars=['Data'], value_vars=df_final.columns[1:], var_name='Variável', value_name='Valor')  # transforma os dados em formato longo
# df_melted = df_melted.dropna(subset=['Valor'])  # remove linhas com valores NaN
# df_melted['Região'] = df_melted['Variável'].str.split(' - ').str[2]  # extrai o estado da variável
# df_melted['Variável'] = df_melted['Variável'].str.split(' - ').str[1]  # extrai a variável

# df_export = df_melted[['Data', 'Variável', 'Região', 'Valor']].copy()  # seleciona as colunas relevantes
# df_export['Data'] = pd.to_datetime(df_export['Data'], format='%m/%Y')  # converte a coluna de data para o formato datetime
# df_export['Valor'] = df_export['Valor'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)  # ajusta o formato dos números
# df_export['Valor'] = df_export['Valor'].astype(float)  # converte a coluna de valor para float

# c.to_csv(df_export, dbs_path, 'bcb_series.csv')  # salva o DataFrame em CSV



# # deflator IPEA IPCA
# try:
#     data = ipeadatapy.timeseries('PRECOS_IPCAG')
#     data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
#     c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
# except Exception as e:
#     errors['IPEA IPCA'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# # gráfico 12.1
# try:
#     # tratamento dos dados
#     data = c.open_file(dbs_path, 'bcb.xlsx', 'xls', sheet_name='Sheet1').query('`Região` == "Sergipe"')  # filtra apenas as linhas com Sergipe
#     data.loc[data['Variável'].str.startswith('Exportação'), 'Variável'] = 'Exportação'
#     data.loc[data['Variável'].str.startswith('Importação'), 'Variável'] = 'Importação'
#     data.loc[data['Variável'].str.startswith('Saldo Comercial'), 'Variável'] = 'Saldo Comercial'
#     data['Data'] = data['Data'].dt.strftime('%d/%m/%Y')  # formata a data para ano-mês

#     df_final = data[['Variável', 'Data', 'Valor']].copy()  # seleciona as colunas relevantes

#     df_final.to_excel(os.path.join(sheets_path, 'g12.1.xlsx'), index=False, sheet_name='g12.1')

# except Exception as e:
#     errors['Gráfico 12.1'] = traceback.format_exc()


# gráfico 12.2
try:
    # tratamento dos dados
    data = c.open_file(dbs_path, 'bcb.xlsx', 'xls', sheet_name='Sheet1')
    data.loc[data['Região'] == 'Brasil', 'Valor'] = data.loc[data['Região'] == 'Brasil', 'Valor'] * 1000
    
    df_merged = data.query('`Região` != "Sergipe"').merge(
        data.query('`Região` == "Sergipe"'), on=['Data', 'Variável'], suffixes=('', '_Sergipe'), validate='m:1'
    )  # join com a tabela de Sergipe
    df_merged['Proporção'] = (df_merged['Valor_Sergipe'] / df_merged['Valor']) * 100  # calcula a proporção de Sergipe em relação ao Brasil
    df_merged['Categoria'] = 'Sergipe/' + df_merged['Região'] + ' (' + df_merged["Variável"].str.lower().str.split().str[0] + ')'  # cria a coluna Categoria

    df_pivoted = pd.pivot(df_merged.query('not `Variável`.str.contains("Saldo")'), index='Data', columns='Categoria', values='Proporção')  # pivota a tabela para ter as categorias como colunas
    df_pivoted.reset_index(inplace=True)  # reseta o índice para que Data seja uma coluna
    cols = df_pivoted.columns.to_list()  # obtém a lista de colunas
    cols = [col.replace('importação', 'importações').replace('exportação', 'exportações') for col in cols]
    df_pivoted.columns = cols  # renomeia as colunas

    df_final = df_pivoted[['Data', 'Sergipe/Brasil (exportações)', 'Sergipe/Nordeste (exportações)', 'Sergipe/Brasil (importações)', 'Sergipe/Nordeste (importações)']].copy()  # seleciona as colunas relevantes
    df_final['Data'] = df_final['Data'].dt.strftime('%d/%m/%Y')  # formata a data para ano-mês
    df_final.dropna(inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g12.2.xlsx'), index=False, sheet_name='g12.2')

except Exception as e:
    errors['Gráfico 12.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g12.1--g12.2--t12.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
