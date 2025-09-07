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
import certifi

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

# # download do arquivo do banco central - bcb
# try:
#     # Sessão persistente para manter os cookies entre requisições
#     session = requests.Session()

#     # URL da consulta
#     consulta_url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarValores"

#     # Parâmetros do formulário
#     year = str(datetime.now().year)
#     form_data = {
#         'optSelecionaSerie': ['13345', '13350', '13356', '13943', '13944', '22708', '22709'],
#         'dataInicio': '01/01/2010',
#         'dataFim': f'31/12/{year}',
#         'selTipoArqDownload': '0',
#         'chkPaginar': 'on',
#         'hdOidSeriesSelecionadas': '13345;13350;13356;13943;13944;22708;22709',
#         'hdPaginar': 'true',
#         'bilServico': '[SGSFW2301]'
#     }

#     # Headers de navegador
#     headers = {
#         'Content-Type': 'application/x-www-form-urlencoded',
#         'Referer': 'https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint',
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
#     }

#     # Envia a requisição de consulta
#     consulta_response = session.post(consulta_url, headers=headers, data=form_data)

#     # Agora vamos simular o download do CSV
#     download_url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=downLoad"

#     download_headers = {
#         'Referer': consulta_url,
#         'User-Agent': headers['User-Agent'],
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#     }

#     # Faz a requisição GET para baixar o CSV
#     csv_response = session.get(download_url, headers=download_headers)
    
#     # Tratamento dos dados
#     df = pd.read_csv(BytesIO(csv_response.content), sep=";", encoding="latin1")
    
#     # Tratamento dos dados
#     df_final = df.loc[~(df[df.columns[0]] == 'Fonte')].copy()  # filtra apenas as linhas com datas válidas
#     df_melted = df_final.melt(id_vars=['Data'], value_vars=df_final.columns[1:], var_name='Variável', value_name='Valor')  # transforma os dados em formato longo
#     df_melted = df_melted.dropna(subset=['Valor'])  # remove linhas com valores NaN
#     df_melted['Região'] = df_melted['Variável'].str.split(' - ').str[2]  # extrai o estado da variável
#     df_melted['Variável'] = df_melted['Variável'].str.split(' - ').str[1]  # extrai a variável

#     df_export = df_melted[['Data', 'Variável', 'Região', 'Valor']].copy()  # seleciona as colunas relevantes
#     df_export['Data'] = pd.to_datetime(df_export['Data'], format='%m/%Y')  # converte a coluna de data para o formato datetime
#     df_export['Valor'] = df_export['Valor'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)  # ajusta o formato dos números
#     df_export['Valor'] = pd.to_numeric(df_export['Valor'], errors='coerce')  # converte a coluna de valor para float, tratando erros
#     df_export.loc[~(df_export['Região'].isin(['Sergipe', 'Nordeste'])), 'Região'] = 'Brasil'  # unifica as regiões Brasil e Nordeste
#     df_export['Variável'] = df_export['Variável'].str.capitalize()  # capitaliza a primeira letra de cada palavra na coluna Variável
#     df_export['Região'] = df_export['Região'].str.capitalize()  # capitaliza a primeira letra de cada palavra na coluna Região

#     c.to_excel(df_export, dbs_path, 'bcb.xlsx')

# except Exception as e:
#     errors['Banco Central - BCB'] = traceback.format_exc()

session = c.create_session_with_retries()

try:
    url = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22%3A%222010%22%2C%22yearEnd%22%3A%222025%22%2C%22typeForm%22%3A3%2C%22typeOrder%22%3A1%2C%22filterList%22%3A%5B%7B%22id%22%3A%22noUf%22%2C%22text%22%3A%22UF%20do%20Produto%22%2C%22route%22%3A%22/pt/location/states%22%2C%22type%22%3A%221%22%2C%22group%22%3A%22gerais%22%2C%22groupText%22%3A%22Gerais%22%2C%22hint%22%3A%22fieldsForm.general.noUf.description%22%2C%22placeholder%22%3A%22UFs%20do%20Produto%22%7D%5D%2C%22filterArray%22%3A%5B%7B%22item%22%3A%5B%2231%22%5D%2C%22idInput%22%3A%22noUf%22%7D%5D%2C%22rangeFilter%22%3A%5B%5D%2C%22detailDatabase%22%3A%5B%5D%2C%22monthDetail%22%3Afalse%2C%22metricFOB%22%3Atrue%2C%22metricKG%22%3Afalse%2C%22metricStatistic%22%3Afalse%2C%22metricFreight%22%3Afalse%2C%22metricInsurance%22%3Afalse%2C%22metricCIF%22%3Afalse%2C%22monthStart%22%3A%2201%22%2C%22monthEnd%22%3A%2212%22%2C%22formQueue%22%3A%22general%22%2C%22langDefault%22%3A%22pt%22%2C%22monthStartName%22%3A%22Janeiro%22%2C%22monthEndName%22%3A%22Dezembro%22%7D"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }
    response = session.get(url, timeout=session.request_timeout, headers=headers, verify=False)
    if response.json()['success']:
        # Salva os dados em Excel para uso posterior
        exports = pd.json_normalize(response.json()['data']['list']['exports'])
        exports['categoria'] = 'Exportação'
        imports = pd.json_normalize(response.json()['data']['list']['imports'])
        imports['categoria'] = 'Importação'
        df = pd.concat([exports, imports], ignore_index=True)
        df.columns = ['ano', 'valor', 'categoria']
        df[['ano', 'valor']] = df[['ano', 'valor']].astype(int)
        
        c.to_csv(df, dbs_path, 'balanca_comercial_sergipe.csv')
except Exception as e:
    errors['Balança comercial Sergipe'] = traceback.format_exc()


try:
    url = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22:%222010%22,%22yearEnd%22:%222025%22,%22typeForm%22:1,%22typeOrder%22:1,%22filterList%22:%5B%7B%22id%22:%22noUf%22,%22text%22:%22UF%20do%20Produto%22,%22route%22:%22/pt/location/states%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22Gerais%22,%22hint%22:%22fieldsForm.general.noUf.description%22,%22placeholder%22:%22UFs%20do%20Produto%22%7D%5D,%22filterArray%22:%5B%7B%22item%22:%5B%2227%22,%2232%22,%2223%22,%2221%22,%2225%22,%2226%22,%2222%22,%2224%22,%2231%22%5D,%22idInput%22:%22noUf%22%7D%5D,%22rangeFilter%22:%5B%5D,%22detailDatabase%22:%5B%5D,%22monthDetail%22:false,%22metricFOB%22:true,%22metricKG%22:false,%22metricStatistic%22:false,%22metricFreight%22:false,%22metricInsurance%22:false,%22metricCIF%22:false,%22monthStart%22:%2201%22,%22monthEnd%22:%2212%22,%22formQueue%22:%22general%22,%22langDefault%22:%22pt%22,%22monthStartName%22:%22Janeiro%22,%22monthEndName%22:%22Dezembro%22%7D"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }
    response = session.get(url, timeout=session.request_timeout, headers=headers, verify=False)
    if response.json()['success']:
        # Salva os dados em Excel para uso posterior
        exports = pd.json_normalize(response.json()['data']['list'])
        exports.columns = ['ano', 'valor']
        exports[['ano', 'valor']] = exports[['ano', 'valor']].astype('int64')
        
        c.to_csv(exports, dbs_path, 'exportacoes_nordeste.csv')
except Exception as e:
    errors['Exportações Nordeste'] = traceback.format_exc()


try:
    url = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22:%222010%22,%22yearEnd%22:%222025%22,%22typeForm%22:1,%22typeOrder%22:1,%22filterList%22:%5B%7B%22id%22:%22noPaispt%22,%22text%22:%22Pa%C3%ADs%22,%22route%22:%22/pt/location/countries%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22Gerais%22,%22hint%22:%22fieldsForm.general.noPais.description%22,%22placeholder%22:%22Pa%C3%ADses%22%7D%5D,%22filterArray%22:%5B%7B%22item%22:%5B%22105%22%5D,%22idInput%22:%22noPaispt%22%7D%5D,%22rangeFilter%22:%5B%5D,%22detailDatabase%22:%5B%5D,%22monthDetail%22:false,%22metricFOB%22:true,%22metricKG%22:false,%22metricStatistic%22:false,%22metricFreight%22:false,%22metricInsurance%22:false,%22metricCIF%22:false,%22monthStart%22:%2201%22,%22monthEnd%22:%2212%22,%22formQueue%22:%22general%22,%22langDefault%22:%22pt%22,%22monthStartName%22:%22Janeiro%22,%22monthEndName%22:%22Dezembro%22%7D"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }
    response = session.get(url, timeout=session.request_timeout, headers=headers, verify=False)
    if response.json()['success']:
        # Salva os dados em Excel para uso posterior
        exports = pd.json_normalize(response.json()['data']['list'])
        exports.columns = ['ano', 'valor']
        exports[['ano', 'valor']] = exports[['ano', 'valor']].astype('int64')
        
        c.to_csv(exports, dbs_path, 'exportacoes_brasil.csv')
except Exception as e:
    errors['Exportações Brasil'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# gráfico 12.1
try:
    # tratamento dos dados
    data = c.open_file(dbs_path, 'balanca_comercial_sergipe.csv', 'csv')
    max_year = data['ano'].max()
    min_year = max_year - 15
    df_pivoted = pd.pivot(data.query(f'ano >= {min_year}'), index='ano', columns='categoria', values='valor').reset_index(drop=False)  # pivota a tabela para ter as categorias como colunas
    df_pivoted['Saldo da balança comercial'] = df_pivoted['Exportação'] - df_pivoted['Importação']
    df_melted = df_pivoted.melt(id_vars=['ano'], value_vars=df_pivoted.columns[1:], var_name='Variável', value_name='Valor')  # transforma os dados em formato longo
    df_melted['Data'] = '01/01/' + df_melted['ano'].astype(str)
    df_final = df_melted[['Variável', 'Data', 'Valor']].copy()  # seleciona as colunas relevantes

    df_final.to_excel(os.path.join(sheets_path, 'g12.1.xlsx'), index=False, sheet_name='g12.1')

except Exception as e:
    errors['Gráfico 12.1'] = traceback.format_exc()

"""
g12.1 foi atualizada, mas quando fui continuar as demais notei uma inconsistência nos dados. Parei no g12.2
exportaçõe de brasil e nordeste foram extraídas; a de sergipe pode ser extraída da balança comercial

"""
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


# # tabela 12.1
# try:
#     # tratamento dos dados
    

#     df_final.to_excel(os.path.join(sheets_path, 't12.1.xlsx'), index=False, sheet_name='t12.1')

# except Exception as e:
#     errors['Tabela 12.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g12.1--g12.2--t12.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
