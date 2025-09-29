"""
Script simples para extrair e processar dados do IPCA
Equivalente ao código R, mas direto e objetivo
"""

import pandas as pd
import requests
from datetime import datetime, date
import json
import functions as c
import os

raw_path = c.raw_path
ipca_filename = 'ipca.parquet'
session = c.create_session_with_retries()

print('Iniciando extração do IPCA...')

# Parte 1: Extração básica do IPCA (índice)
print('Baixando índice IPCA...')
url_ipca = 'https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/all/p/all/d/v2266%2013'
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