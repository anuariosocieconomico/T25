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
    url = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22%3A%222010%22%2C%22yearEnd%22%3A%222025%22%2C%22typeForm%22%3A3%2C%22typeOrder%22%3A1%2C%22filterList%22%3A%5B%7B%22id%22%3A%22noUf%22%2C%22text%22%3A%22UF%20do%20Produto%22%2C%22route%22%3A%22/pt/location/states%22%2C%22type%22%3A%221%22%2C%22group%22%3A%22gerais%22%2C%22groupText%22%3A%22Gerais%22%2C%22hint%22%3A%22fieldsForm.general.noUf.description%22%2C%22placeholder%22%3A%22UFs%20do%20Produto%22%7D%5D%2C%22filterArray%22%3A%5B%7B%22item%22%3A%5B%2212%22%2C%2227%22%2C%2216%22%2C%2213%22%2C%2232%22%2C%2223%22%2C%2254%22%2C%2234%22%2C%2253%22%2C%2221%22%2C%2252%22%2C%2255%22%2C%2233%22%2C%2242%22%2C%2225%22%2C%2215%22%2C%2226%22%2C%2222%22%2C%2224%22%2C%2245%22%2C%2236%22%2C%2211%22%2C%2214%22%2C%2244%22%2C%2231%22%2C%2241%22%2C%2217%22%5D%2C%22idInput%22%3A%22noUf%22%7D%5D%2C%22rangeFilter%22%3A%5B%5D%2C%22detailDatabase%22%3A%5B%5D%2C%22monthDetail%22%3Afalse%2C%22metricFOB%22%3Atrue%2C%22metricKG%22%3Afalse%2C%22metricStatistic%22%3Afalse%2C%22metricFreight%22%3Afalse%2C%22metricInsurance%22%3Afalse%2C%22metricCIF%22%3Afalse%2C%22monthStart%22%3A%2201%22%2C%22monthEnd%22%3A%2212%22%2C%22formQueue%22%3A%22general%22%2C%22langDefault%22%3A%22pt%22%2C%22monthStartName%22%3A%22Janeiro%22%2C%22monthEndName%22%3A%22Dezembro%22%7D"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
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
        df[['ano', 'valor']] = df[['ano', 'valor']].astype('Int64')
        df['uf'] = 'Brasil'
        
        c.to_csv(df, dbs_path, 'balanca_comercial_brasil.csv')
except Exception as e:
    errors['Balança comercial - Brasil'] = traceback.format_exc()


try:
    url_sergipe = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22%3A%222010%22%2C%22yearEnd%22%3A%222025%22%2C%22typeForm%22%3A3%2C%22typeOrder%22%3A1%2C%22filterList%22%3A%5B%7B%22id%22%3A%22noUf%22%2C%22text%22%3A%22UF%20do%20Produto%22%2C%22route%22%3A%22/pt/location/states%22%2C%22type%22%3A%221%22%2C%22group%22%3A%22gerais%22%2C%22groupText%22%3A%22Gerais%22%2C%22hint%22%3A%22fieldsForm.general.noUf.description%22%2C%22placeholder%22%3A%22UFs%20do%20Produto%22%7D%5D%2C%22filterArray%22%3A%5B%7B%22item%22%3A%5B%2231%22%5D%2C%22idInput%22%3A%22noUf%22%7D%5D%2C%22rangeFilter%22%3A%5B%5D%2C%22detailDatabase%22%3A%5B%5D%2C%22monthDetail%22%3Afalse%2C%22metricFOB%22%3Atrue%2C%22metricKG%22%3Afalse%2C%22metricStatistic%22%3Afalse%2C%22metricFreight%22%3Afalse%2C%22metricInsurance%22%3Afalse%2C%22metricCIF%22%3Afalse%2C%22monthStart%22%3A%2201%22%2C%22monthEnd%22%3A%2212%22%2C%22formQueue%22%3A%22general%22%2C%22langDefault%22%3A%22pt%22%2C%22monthStartName%22%3A%22Janeiro%22%2C%22monthEndName%22%3A%22Dezembro%22%7D"
    headers_sergipe = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    response_sergipe = session.get(url_sergipe, timeout=session.request_timeout, headers=headers_sergipe, verify=False)
    if response_sergipe.json()['success']:
        exports_sergipe = pd.json_normalize(response_sergipe.json()['data']['list']['exports'])
        exports_sergipe['categoria'] = 'Exportação'
        imports_sergipe = pd.json_normalize(response_sergipe.json()['data']['list']['imports'])
        imports_sergipe['categoria'] = 'Importação'
        df_sergipe = pd.concat([exports_sergipe, imports_sergipe], ignore_index=True)
        df_sergipe.columns = ['ano', 'valor', 'categoria']
        df_sergipe[['ano', 'valor']] = df_sergipe[['ano', 'valor']].astype('Int64')
        df_sergipe['uf'] = 'Sergipe'
        c.to_csv(df_sergipe, dbs_path, 'balanca_comercial_sergipe.csv')
except Exception as e:
    errors['Balança comercial - Sergipe'] = traceback.format_exc()


try:
    url_nordeste = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22%3A%222010%22%2C%22yearEnd%22%3A%222025%22%2C%22typeForm%22%3A3%2C%22typeOrder%22%3A1%2C%22filterList%22%3A%5B%7B%22id%22%3A%22noUf%22%2C%22text%22%3A%22UF%20do%20Produto%22%2C%22route%22%3A%22/pt/location/states%22%2C%22type%22%3A%221%22%2C%22group%22%3A%22gerais%22%2C%22groupText%22%3A%22Gerais%22%2C%22hint%22%3A%22fieldsForm.general.noUf.description%22%2C%22placeholder%22%3A%22UFs%20do%20Produto%22%7D%5D%2C%22filterArray%22%3A%5B%7B%22item%22%3A%5B%2231%22%2C%2227%22%2C%2232%22%2C%2223%22%2C%2225%22%2C%2226%22%2C%2224%22%2C%2221%22%2C%2222%22%5D%2C%22idInput%22%3A%22noUf%22%7D%5D%2C%22rangeFilter%22%3A%5B%5D%2C%22detailDatabase%22%3A%5B%5D%2C%22monthDetail%22%3Afalse%2C%22metricFOB%22%3Atrue%2C%22metricKG%22%3Afalse%2C%22metricStatistic%22%3Afalse%2C%22metricFreight%22%3Afalse%2C%22metricInsurance%22%3Afalse%2C%22metricCIF%22%3Afalse%2C%22monthStart%22%3A%2201%22%2C%22monthEnd%22%3A%2212%22%2C%22formQueue%22%3A%22general%22%2C%22langDefault%22%3A%22pt%22%2C%22monthStartName%22%3A%22Janeiro%22%2C%22monthEndName%22%3A%22Dezembro%22%7D"
    headers_nordeste = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    response_nordeste = session.get(url_nordeste, timeout=session.request_timeout, headers=headers_nordeste, verify=True)
    if response_nordeste.json()['success']:
        exports_nordeste = pd.json_normalize(response_nordeste.json()['data']['list']['exports'])
        exports_nordeste['categoria'] = 'Exportação'
        imports_nordeste = pd.json_normalize(response_nordeste.json()['data']['list']['imports'])
        imports_nordeste['categoria'] = 'Importação'
        df_nordeste = pd.concat([exports_nordeste, imports_nordeste], ignore_index=False)
        df_nordeste.columns = ['ano', 'valor', 'categoria']
        df_nordeste[['ano', 'valor']] = df_nordeste[['ano', 'valor']].astype('Int64')
        df_nordeste['uf'] = 'Nordeste'
        c.to_csv(df_nordeste, dbs_path, 'balanca_comercial_nordeste.csv')
except Exception as e:
    errors['Balança comercial - Nordeste'] = traceback.format_exc()

# Fonte da tabela 12.1 a partir daqui
try:
    url_cuci = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22%3A%222010%22%2C%22yearEnd%22%3A%222025%22%2C%22typeForm%22%3A1%2C%22typeOrder%22%3A1%2C%22filterList%22%3A%5B%7B%22id%22%3A%22noUf%22%2C%22text%22%3A%22UF%20do%20Produto%22%2C%22route%22%3A%22/pt/location/states%22%2C%22type%22%3A%221%22%2C%22group%22%3A%22gerais%22%2C%22groupText%22%3A%22Gerais%22%2C%22hint%22%3A%22fieldsForm.general.noUf.description%22%2C%22placeholder%22%3A%22UFs%20do%20Produto%22%7D%5D%2C%22filterArray%22%3A%5B%7B%22item%22%3A%5B%2231%22%5D%2C%22idInput%22%3A%22noUf%22%7D%5D%2C%22rangeFilter%22%3A%5B%5D%2C%22detailDatabase%22%3A%5B%7B%22id%22%3A%22noCuciPospt%22%2C%22text%22%3A%22CUCI%20Grupo%20(produtos)%22%2C%22parentId%22%3A%22coCuciPos%22%2C%22parent%22%3A%22C%C3%B3digo%20CUCI%20Grupo%22%2C%22group%22%3A%22cuci%22%2C%22groupText%22%3A%22Classifica%C3%A7%C3%A3o%20Uniforme%20para%20o%20Com%C3%A9rcio%20Internacional%20(CUCI%20Ver.3)%22%7D%5D%2C%22monthDetail%22%3Afalse%2C%22metricFOB%22%3Atrue%2C%22metricKG%22%3Afalse%2C%22metricStatistic%22%3Afalse%2C%22metricFreight%22%3Afalse%2C%22metricInsurance%22%3Afalse%2C%22metricCIF%22%3Afalse%2C%22monthStart%22%3A%2201%22%2C%22monthEnd%22%3A%2212%22%2C%22formQueue%22%3A%22general%22%2C%22langDefault%22%3A%22pt%22%2C%22monthStartName%22%3A%22Janeiro%22%2C%22monthEndName%22%3A%22Dezembro%22%7D"
    headers_cuci = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    response_cuci = session.get(url_cuci, timeout=session.request_timeout, headers=headers_cuci, verify=False)
    if response_cuci.json().get('success'):
        exports_cuci = pd.json_normalize(response_cuci.json()['data']['list'])
        exports_cuci.columns = ['ano', 'código', 'produto', 'valor']
        exports_cuci[['ano', 'valor']] = exports_cuci[['ano', 'valor']].astype('Int64')
        exports_cuci['uf'] = 'Sergipe'
        c.to_csv(exports_cuci, dbs_path, 'exportações_produtos_sergipe_cuci.csv')
except Exception as e:
    errors['Exportações produtos - Sergipe'] = traceback.format_exc()


try:
    url_ufs = "https://api-comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22%3A%222010%22%2C%22yearEnd%22%3A%222025%22%2C%22typeForm%22%3A1%2C%22typeOrder%22%3A1%2C%22filterList%22%3A%5B%7B%22id%22%3A%22noUf%22%2C%22text%22%3A%22UF%20do%20Produto%22%2C%22route%22%3A%22/pt/location/states%22%2C%22type%22%3A%221%22%2C%22group%22%3A%22gerais%22%2C%22groupText%22%3A%22Gerais%22%2C%22hint%22%3A%22fieldsForm.general.noUf.description%22%2C%22placeholder%22%3A%22UFs%20do%20Produto%22%7D%5D%2C%22filterArray%22%3A%5B%7B%22item%22%3A%5B%2231%22%2C%2212%22%2C%2227%22%2C%2216%22%2C%2213%22%2C%2232%22%2C%2223%22%2C%2254%22%2C%2234%22%2C%2253%22%2C%2221%22%2C%2252%22%2C%2255%22%2C%2233%22%2C%2242%22%2C%2225%22%2C%2215%22%2C%2226%22%2C%2222%22%2C%2224%22%2C%2245%22%2C%2236%22%2C%2211%22%2C%2214%22%2C%2244%22%2C%2241%22%2C%2217%22%5D%2C%22idInput%22%3A%22noUf%22%7D%5D%2C%22rangeFilter%22%3A%5B%5D%2C%22detailDatabase%22%3A%5B%5D%2C%22monthDetail%22%3Afalse%2C%22metricFOB%22%3Atrue%2C%22metricKG%22%3Afalse%2C%22metricStatistic%22%3Afalse%2C%22metricFreight%22%3Afalse%2C%22metricInsurance%22%3Afalse%2C%22metricCIF%22%3Afalse%2C%22monthStart%22%3A%2201%22%2C%22monthEnd%22%3A%2212%22%2C%22formQueue%22%3A%22general%22%2C%22langDefault%22%3A%22pt%22%2C%22monthStartName%22%3A%22Janeiro%22%2C%22monthEndName%22%3A%22Dezembro%22%7D"
    headers_ufs = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "dnt": "1",
        "origin": "https://comexstat.mdic.gov.br",
        "priority": "u=1, i",
        "referer": "https://comexstat.mdic.gov.br/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    response_ufs = session.get(url_ufs, timeout=session.request_timeout, headers=headers_ufs, verify=False)
    if response_ufs.json().get('success'):
        exports_ufs = pd.json_normalize(response_ufs.json()['data']['list'])
        exports_ufs.columns = ['ano', 'valor']
        exports_ufs[['ano', 'valor']] = exports_ufs[['ano', 'valor']].astype('Int64')
        exports_ufs['uf'] = 'Brasil'
        c.to_csv(exports_ufs, dbs_path, 'exportacoes_produtos_brasil.csv')
except Exception as e:
    errors['Exportações produtos - Brasil'] = traceback.format_exc()


# produto interno bruto
try:
    consulta_url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarValores"
    headers_bcb = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "content-type": "application/x-www-form-urlencoded",
        "dnt": "1",
        "origin": "https://www3.bcb.gov.br",
        "priority": "u=0, i",
        "referer": "https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    cookies_bcb = {
        "SGS/ConfiguracoesAmbiente": "P/E/E",
        "TS019aa75b": "012e4f88b3b165ae605bfc9da4d3c61ca4cb8260b2a700b8283637fbc421374abaa0e34e884e0bc0975ace116d5f2bddbb36fdf06a4801812e891f8a172339d7d2f8ef9c48",
        "JSESSIONID": "0000cnuh7xV1Xrmdp0oEklQIIFp:1dai8m94c",
        "BIGipServer~was_p_as3~was_p~pool_was_443_p": "1020268972.47873.0000",
        "BIGipServer~www3_p_as3~www3_p~pool_dmz-vs-pwas_443_p": "!0f5FQqhfkrJ0FoMNYXpg27kkhFW1VVUIExbn/gqmyLLwXHG3qnFheo8ToDAUY2CrCai2wrYEr8iqb5g=",
        "TS012b7e98": "012e4f88b33c4f4316f1be06da455ec36226670c33a700b8283637fbc421374abaa0e34e885db388615e1f3cb2102b614c8419188f8535e8540ba27f4eefa93239d418e7ec442e67454c01f7b83b434623d582d543e748c068b2e609c2363afdeb92b0ef47"
    }
    form_data_bcb = {
        "optSelecionaSerie": "7324",
        "dataInicio": "01/01/2006",
        "dataFim": "19/09/2025",
        "selTipoArqDownload": "0",
        "chkPaginar": "on",
        "hdOidSeriesSelecionadas": "7324",
        "hdPaginar": "true",
        "bilServico": "[SGSFW2301]"
    }
    session_bcb = requests.Session()
    max_retries = 3
    timeout = 60

    for attempt in range(max_retries):
        try:
            response_bcb = session_bcb.post(
                consulta_url,
                headers=headers_bcb,
                cookies=cookies_bcb,
                data=form_data_bcb,
                timeout=timeout,
                verify=True
            )
            if response_bcb.status_code == 200:
                break
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

    download_url_bcb = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=downLoad"
    download_headers_bcb = headers_bcb.copy()
    download_headers_bcb["referer"] = consulta_url

    for attempt in range(max_retries):
        try:
            csv_response_bcb = session_bcb.get(
                download_url_bcb,
                headers=download_headers_bcb,
                cookies=cookies_bcb,
                timeout=timeout,
                verify=True
            )
            if csv_response_bcb.status_code == 200:
                break
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

    df_bcb = pd.read_csv(BytesIO(csv_response_bcb.content), sep=";", encoding="latin1")
    df_final = df_bcb.loc[~(df_bcb[df_bcb.columns[0]] == 'Fonte')].copy()
    df_final.columns = ['ano', 'valor']
    df_final['valor'] = df_final['valor'].str.replace('.', '').str.replace(',', '.').astype(float)

    c.to_excel(df_final, dbs_path, 'bcb.xlsx')
except Exception as e:
    errors['Banco Central - BCB'] = traceback.format_exc()


# contas da produção
try:
    year = datetime.now().year
    while year >= 2020:
        try:
            # url da base contas regionais
            url = f'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais/{year}/xls'
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            content = pd.DataFrame(response.json())
            link = content.query(
                'name.str.lower().str.startswith("conta_da_producao_2010") and name.str.lower().str.endswith(".zip")'
            )['url'].values[0]
            response = session.get(link, timeout=session.request_timeout, headers=c.headers)
            c.to_file(dbs_path, 'ibge_conta_producao.zip', response.content)
            break
        
        except:
            year -= 1
    
    if response.status_code == 200:
        print(f'Download da base "Contas da Produção" realizado com sucesso para o ano de {year}.')

except Exception as e:
    errors[url + ' (Conta da Produção)'] = traceback.format_exc()


try:
    url_bcb = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarValores"
    headers_bcb_curl = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "content-type": "application/x-www-form-urlencoded",
        "dnt": "1",
        "origin": "https://www3.bcb.gov.br",
        "priority": "u=0, i",
        "referer": "https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    cookies_bcb_curl = {
        "SGS/ConfiguracoesAmbiente": "P/E/E",
        "TS019aa75b": "012e4f88b3b165ae605bfc9da4d3c61ca4cb8260b2a700b8283637fbc421374abaa0e34e884e0bc0975ace116d5f2bddbb36fdf06a4801812e891f8a172339d7d2f8ef9c48",
        "JSESSIONID": "0000cnuh7xV1Xrmdp0oEklQIIFp:1dai8m94c",
        "BIGipServer~was_p_as3~was_p~pool_was_443_p": "1020268972.47873.0000",
        "BIGipServer~www3_p_as3~www3_p~pool_dmz-vs-pwas_443_p": "!0f5FQqhfkrJ0FoMNYXpg27kkhFW1VVUIExbn/gqmyLLwXHG3qnFheo8ToDAUY2CrCai2wrYEr8iqb5g=",
        "TS012b7e98": "012e4f88b3eb0667460245e1d349fb862b47840bdbb68210c53b32e24a1291859ff615773f8dbeeb953df799b50e77bcb805ab5640cfce12e85349441249f9b85369818b0d9c0d3192adb79f38ed8943edf63f0c934e8fa3fdb5ae7d67fafb2b3221450b6a"
    }
    form_data_bcb_curl = {
        "optSelecionaSerie": "4386",
        "dataInicio": "01/01/2010",
        "dataFim": "20/09/2025",
        "selTipoArqDownload": "0",
        "chkPaginar": "on",
        "hdOidSeriesSelecionadas": "4386",
        "hdPaginar": "true",
        "bilServico": "[SGSFW2301]"
    }
    session_bcb_curl = requests.Session()
    max_retries = 3
    timeout = 60

    for attempt in range(max_retries):
        try:
            response_bcb_curl = session_bcb_curl.post(
                url_bcb,
                headers=headers_bcb_curl,
                cookies=cookies_bcb_curl,
                data=form_data_bcb_curl,
                timeout=timeout,
                verify=True
            )
            if response_bcb_curl.status_code == 200:
                break
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

    download_url_bcb_curl = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=downLoad"
    for attempt in range(max_retries):
        try:
            csv_response_bcb_curl = session_bcb_curl.get(
                download_url_bcb_curl,
                headers=headers_bcb_curl,
                cookies=cookies_bcb_curl,
                timeout=timeout,
                verify=True
            )
            if csv_response_bcb_curl.status_code == 200:
                break
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

    df_bcb_curl = pd.read_csv(BytesIO(csv_response_bcb_curl.content), sep=";", encoding="latin1")
    df_final = df_bcb_curl.loc[~(df_bcb_curl[df_bcb_curl.columns[0]] == 'Fonte')].copy()
    df_final.columns = ['data', 'valor']
    df_final['valor'] = df_final['valor'].str.replace('.', '').astype(int)
    df_final['ano'] = pd.to_datetime(df_final['data'], format='%m/%Y').dt.year
    df_final['mes'] = pd.to_datetime(df_final['data'], format='%m/%Y').dt.month


    c.to_excel(df_final, dbs_path, 'bcb_3693.xlsx')
except Exception as e:
    errors['Banco Central - BCB (curl)'] = traceback.format_exc()

# ************************
# PLANILHA
# ************************

# gráfico 12.1
try:
    # tratamento dos dados
    data = c.open_file(dbs_path, 'balanca_comercial_sergipe.csv', 'csv')
    max_year = data['ano'].max()
    min_year = max_year - 14
    df_pivoted = pd.pivot(data.query(f'ano >= {min_year}'), index='ano', columns='categoria', values='valor').reset_index(drop=False)  # pivota a tabela para ter as categorias como colunas
    df_pivoted['Saldo da balança comercial'] = df_pivoted['Exportação'] - df_pivoted['Importação']
    df_melted = df_pivoted.melt(id_vars=['ano'], value_vars=df_pivoted.columns[1:], var_name='Variável', value_name='Valor')  # transforma os dados em formato longo
    df_melted['Data'] = '01/01/' + df_melted['ano'].astype(str)
    df_final = df_melted[['Variável', 'Data', 'Valor']].copy()  # seleciona as colunas relevantes

    df_final.to_excel(os.path.join(sheets_path, 'g12.1.xlsx'), index=False, sheet_name='g12.1')

except Exception as e:
    errors['Gráfico 12.1'] = traceback.format_exc()


# gráfico 12.2
try:
    """
    transformação da antiga fonte
    """
    # # tratamento dos dados
    # data = c.open_file(dbs_path, 'bcb.xlsx', 'xls', sheet_name='Sheet1')
    # data.loc[data['Região'] == 'Brasil', 'Valor'] = data.loc[data['Região'] == 'Brasil', 'Valor'] * 1000
    
    # df_merged = data.query('`Região` != "Sergipe"').merge(
    #     data.query('`Região` == "Sergipe"'), on=['Data', 'Variável'], suffixes=('', '_Sergipe'), validate='m:1'
    # )  # join com a tabela de Sergipe
    # df_merged['Proporção'] = (df_merged['Valor_Sergipe'] / df_merged['Valor']) * 100  # calcula a proporção de Sergipe em relação ao Brasil
    # df_merged['Categoria'] = 'Sergipe/' + df_merged['Região'] + ' (' + df_merged["Variável"].str.lower().str.split().str[0] + ')'  # cria a coluna Categoria

    # df_pivoted = pd.pivot(df_merged.query('not `Variável`.str.contains("Saldo")'), index='Data', columns='Categoria', values='Proporção')  # pivota a tabela para ter as categorias como colunas
    # df_pivoted.reset_index(inplace=True)  # reseta o índice para que Data seja uma coluna
    # cols = df_pivoted.columns.to_list()  # obtém a lista de colunas
    # cols = [col.replace('importação', 'importações').replace('exportação', 'exportações') for col in cols]
    # df_pivoted.columns = cols  # renomeia as colunas

    # df_final = df_pivoted[['Data', 'Sergipe/Brasil (exportações)', 'Sergipe/Nordeste (exportações)', 'Sergipe/Brasil (importações)', 'Sergipe/Nordeste (importações)']].copy()  # seleciona as colunas relevantes
    # df_final['Data'] = df_final['Data'].dt.strftime('%d/%m/%Y')  # formata a data para ano-mês
    # df_final.dropna(inplace=True)

    # df_final.to_excel(os.path.join(sheets_path, 'g12.2.xlsx'), index=False, sheet_name='g12.2')

    data_brasil = c.open_file(dbs_path, 'balanca_comercial_brasil.csv', 'csv')
    data_nordeste = c.open_file(dbs_path, 'balanca_comercial_nordeste.csv', 'csv')
    max_year = data_brasil['ano'].max()
    min_year = max_year - 14
    data_sergipe = c.open_file(dbs_path, 'balanca_comercial_sergipe.csv', 'csv').query('ano >= @min_year')  # filtra apenas exportações
    df_concat = pd.concat([data_brasil, data_nordeste], ignore_index=True).query('ano >= @min_year')  # concatena as bases de dados e filtra apenas exportações
    df_merged = df_concat.merge(
        data_sergipe[['ano', 'categoria', 'valor']], on=['ano', 'categoria'], suffixes=('', '_Sergipe'), validate='m:1'
    )  # join com a tabela de Sergipe
    df_merged['Proporção'] = (df_merged['valor_Sergipe'] / df_merged['valor']) * 100  # calcula a proporção de Sergipe em relação ao Brasil
    df_merged['Categoria'] = 'Sergipe/' + df_merged['uf'] + ' (' + df_merged["categoria"].str.lower() + ')'  # cria a coluna Categoria
    df_merged['Categoria'] = df_merged['Categoria'].str.replace('importação', 'importações').str.replace('exportação', 'exportações')
    df_pivoted = pd.pivot(df_merged, index='ano', columns='Categoria', values='Proporção').reset_index(drop=False)  # pivota a tabela para ter as categorias como colunas
    df_pivoted['Data'] = '01/01/' + df_pivoted['ano'].astype(str)
    df_final = df_pivoted[['Data', 'Sergipe/Brasil (exportações)', 'Sergipe/Nordeste (exportações)', 'Sergipe/Brasil (importações)', 'Sergipe/Nordeste (importações)']].copy()  # seleciona as colunas relevantes
    df_final.dropna(inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g12.2.xlsx'), index=False, sheet_name='g12.2')

except Exception as e:
    errors['Gráfico 12.2'] = traceback.format_exc()

"""
ao multiplicar exportações de sergipe pela taxa média de cmabio, os valores ficam na casa dos trilhões; validar com o professor
"""
# # tabela 12.1
# try:
#     df_produtos_sergipe = c.open_file(dbs_path, 'exportações_produtos_sergipe_cuci.csv', 'csv')
#     df_produtos_brasil = c.open_file(dbs_path, 'exportacoes_produtos_brasil.csv', 'csv')
#     df_pib_brasil = c.open_file(dbs_path, 'bcb.xlsx', 'xls', sheet_name='Sheet1')
#     df_pib_sergipe = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.1', skiprows=43)
#     df_taxa_cambio = c.open_file(dbs_path, 'bcb_3693.xlsx', 'xls', sheet_name='Sheet1')

#     # tratamento produtos
#     min_year = df_produtos_sergipe['ano'].max() - 14
#     df_produtos_sergipe = df_produtos_sergipe.query('ano >= @min_year').copy()
#     df_produtos_brasil = df_produtos_brasil.query('ano >= @min_year').copy()

#     # tratamento de df_pib_sergipe
#     col0 = df_pib_sergipe.columns[0]
#     df_pib_sergipe = df_pib_sergipe.loc[~(df_pib_sergipe[col0].astype(str).str.startswith('Fonte'))].copy()
#     df_pib_sergipe.columns = [col.split('\n')[0].strip() for col in df_pib_sergipe.columns]

#     # tratamento taxa de cambio
#     df_taxa_cambio = df_taxa_cambio.groupby('ano')['valor'].mean().reset_index()

#     # união de produtos sergipe e taxa de cambio
#     df_merged = df_produtos_sergipe.merge(df_taxa_cambio, on='ano', validate='m:1', suffixes=('', '_taxa_cambio'))
#     df_merged['exportacao/taxa'] = df_merged['valor'] * df_merged['valor_taxa_cambio']

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
