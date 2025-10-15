import datetime
import traceback

import numpy as np
import functions as c
import os
import pandas as pd
import requests
from lxml import html as lxml_html
import gdown
import tempfile
import shutil


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
error_path = c.error_path
temp_folder = tempfile.mkdtemp()
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
db_name = 'caged'  # nome base da base de dados
file_name = f'raw_{db_name}.parquet'  # nome do arquivo parquet
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    
    session = c.create_session_with_retries()
    url = 'https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/novo-caged'
    html = session.get(url, timeout=session.request_timeout, headers=c.headers).text

    tree = lxml_html.fromstring(html)
    container = tree.xpath('//div[contains(@class, "cover-richtext-tile") and contains(@class, "tile-content")]')[-1]

    # Busca o link das tabelas Excel no Google Drive
    drive_link = None
    for element in container.xpath('.//li/a'):
        texto = element.text_content().lower().strip()
        if 'tabela' in texto:
            drive_link = element.get('href')
            break
    
    # Baixa o arquivo Excel do Google Drive usando gdown e armazena na pasta temporária
    gdown.download_folder(drive_link, output=temp_folder, quiet=False, use_cookies=False)
    print('Arquivos baixados com sucesso!')

    # Retorna erro se não encontrar exatamente um arquivo na pasta temporária
    if len(os.listdir(temp_folder)) != 1:
        raise ValueError("Mais de um link encontrado ou nenhum link encontrado.")
    else:
        excel_file = os.path.join(temp_folder, os.listdir(temp_folder)[0])
        data = pd.read_excel(excel_file, sheet_name='Tabela 8.1', skiprows=4)

    # tabela pivotada, onde cada mês tem 4 ou 5 colunas (estoque, admissões, desligamentos, saldo e variação)
    # transformações adicionais foram necessárias para lidar com essa estrutura
    months = [
        'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
        'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro',
        'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'
    ]
    cols = data.columns.tolist()
    month_cols = [col for col in cols if col.lower().split('/')[0] in months]  # filtra apenas as colunas que correspondem a meses
    n_cols = len(month_cols)

    # cria uma nova lista de colunas, repetindo cada mês 4 ou 5 vezes conforme necessário
    # apenas o primeiro mês da série tem 4 colunas, os demais têm 5
    new_list = ['UF', 'COD', 'Município']
    for i, col in enumerate(month_cols):
        if i == 0:
            new_list.extend([col] * 4)
        else:
            new_list.extend([col] * 5)

    # carrega o DataFrame novamente, evitando agora aa linha de cabeçalho extra
    df = pd.read_excel(excel_file, sheet_name='Tabela 8.1', skiprows=5)
    df.drop(columns=[df.columns[0]], inplace=True)  # remove a primeira coluna vazia

    new_list.extend([''] * (len(df.columns) - len(new_list)))  # completa a lista com strings vazias para as últimas colunas que são agregados do ano

    # renomeia as colunas; as três primeiras são alteradas por ['UF', 'COD', 'Município']
    # já as demais são renomeadas para col + mês, como 'Estoque - Janeiro/2024'
    for i, (col, info) in enumerate(zip(df.columns, new_list)):
        if i <= 2:
            df.rename(columns={col: info}, inplace=True)
        else:
            new_col_name = col.split('.')  # remove qualquer numeração decimal que possa existir, em caso de repetição de nomes

            # se houver mais de um elemento na nova coluna, significa que houve foi adicionado um sufixo decimal para diferenciar as colunas
            if len(new_col_name) > 1:
                new_col_name = new_col_name[0].strip() + ' - ' + info
            else:
                new_col_name = col.strip() + ' - ' + info

            # uma nova camada foi adicionada aqui para garantir que não haja colunas com nomes duplicados
            # isto pode ocorrer para as colunas finais que são agregados do ano, não contendo a informação do mês para diferenciar
            tries = 0
            while tries < 5 or new_col_name not in df.columns:
                if new_col_name not in df.columns:
                    df.rename(columns={col: new_col_name}, inplace=True)
                    break
                else:
                    tries += 1
                    new_col_name += ' <> ' + str(tries)  # adiciona um sufixo para diferenciar as colunas
                    

    # Salva o arquivo parquet
    for col in df.columns:
        if col in ['UF', 'COD', 'Município']:
            df[col] = df[col].astype('category')
        else:
            df[col] = df[col].replace('--', np.nan).astype('float64')

    df.to_parquet(file_path, index=False)
    print(f"\nDados salvos em {file_path}")

    # Remove a pasta temporária
    shutil.rmtree(temp_folder)

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_caged.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_caged.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_caged.txt".')