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
        'optSelecionaSerie': ['13345', '13350', '13356'],
        'dataInicio': '01/01/2010',
        'dataFim': f'31/12/{year}',
        'selTipoArqDownload': '0',
        'chkPaginar': 'on',
        'hdOidSeriesSelecionadas': '13345;13350;13356',
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
    df = df.loc[~(df[df.columns[0]] == 'Fonte')].copy()  # filtra apenas as linhas com datas válidas
    df['Data'] = pd.to_datetime(df['Data'], format='%m/%Y')  # converte a coluna de data para o formato datetime
    df.columns = ['Data', 'Exportação', 'Importação', 'Saldo Comercial']  # renomeia as colunas
    df[['Exportação', 'Importação', 'Saldo Comercial']] = df[['Exportação', 'Importação', 'Saldo Comercial']].astype(float)  # converte as colunas de valores para float

    c.to_excel(df, dbs_path, 'bcb.xlsx')

except Exception as e:
    errors['Banco Central - BCB'] = traceback.format_exc()


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

# gráfico 12.1
try:
    # tratamento dos dados
    data = c.open_file(dbs_path, 'bcb.xlsx', 'xls', sheet_name='Sheet1')
    df_melted = data.melt(id_vars=['Data'], value_vars=['Exportação', 'Importação', 'Saldo Comercial'], var_name='Variável', value_name='Valor')  # transforma os dados em formato longo
    df_melted['Data'] = df_melted['Data'].dt.strftime('%d/%m/%Y')  # formata a data
    df_melted['Valor'] = df_melted['Valor'] * 1000  # converte os valores para milhares
    df_melted = df_melted[['Variável', 'Data', 'Valor']]  # reordena as colunas

    df_melted.to_excel(os.path.join(sheets_path, 'g12.1.xlsx'), index=False, sheet_name='g12.1')

except Exception as e:
    errors['Gráfico 12.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g12.1--g12.2--t12.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
