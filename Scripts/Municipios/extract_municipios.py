import traceback
import functions as c
import os
import pandas as pd
from datetime import datetime, date
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(error_path, exist_ok=True)
os.makedirs(raw_path, exist_ok=True)
file_name = 'municipios.csv'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    url = 'https://raw.githubusercontent.com/Rodslater/cauc/5c7fb11f0d117672f92f8606b437f7dc04b0fbe0/data/municipios.csv'
    session = c.create_session_with_retries()
    response = session.get(url, timeout=session.request_timeout, headers=session.headers)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
    else:
        print(f"Erro ao acessar os dados: Status {response.status_code}")

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_extract_municipios.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em extract_municipios.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_extract_municipios.txt".')
