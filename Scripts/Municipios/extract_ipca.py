"""
Script simples para extrair e processar dados do IPCA
Equivalente ao código R, mas direto e objetivo
"""

import traceback
import pandas as pd
import requests
from datetime import datetime, date
import json
import functions as c
import os

raw_path = c.raw_path
error_path = c.error_path
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
ipca_filename = 'ipca.parquet'
session = c.create_session_with_retries()

try:
    print('Iniciando extração do IPCA...')

    # Parte 1: Extração básica do IPCA (índice)
    print('Baixando índice IPCA...')
    url_ipca = 'https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/2266/p/all/d/v2266%2013'
    response = session.get(url_ipca, timeout=session.request_timeout, headers=c.headers)
    data_ipca = pd.DataFrame(response.json())

    # Processar dados básicos do IPCA
    data_ipca.columns = data_ipca.iloc[0]
    ipca = data_ipca.iloc[1:][['Mês (Código)', 'Valor']].copy()
    ipca = ipca.rename(columns={'Valor': 'indice_ipca', 'Mês (Código)': 'Mes'})
    ipca['indice_ipca'] = pd.to_numeric(ipca['indice_ipca'], errors='coerce')

    # Converter código do mês (YYYYMM) para data
    ipca['Mes'] = pd.to_datetime(ipca['Mes'].astype(str) + '01', format='%Y%m%d')
    ipca = ipca.dropna()
    ipca.to_parquet(os.path.join(raw_path, ipca_filename), index=False)

    print(f'IPCA básico: {len(ipca)} registros')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_extract_ipca.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em extract_ipca.py em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao extrair o IPCA. Verifique o log em "Doc/Municipios/log_extract_ipca.txt".')