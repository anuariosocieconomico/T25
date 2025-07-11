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


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# sidra 5906
url = 'https://apisidra.ibge.gov.br/values/t/5906/n1/all/n3/all/v/7168/p/all/c11046/all/d/v7168%205?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D4N', 'V']].copy()
    df.columns = ['Data', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Ano'] = df['Data'].str.split(' ').str[-1]  # extrai o ano da coluna Data
    df['Ano'] = df['Ano'].astype(int)  # converte a coluna Ano para inteiro
    df['Mês'] = df['Data'].str.split(' ').str[0]  # extrai o mês da coluna Data

    c.to_excel(df, dbs_path, 'sidra_5906.xlsx')
except Exception as e:
    errors['Sidra 5906'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# gráfico 10.1
try:
    data = c.open_file(dbs_path, 'sidra_5906.xlsx', 'xls', sheet_name='Sheet1').query(
        '`Mês` == "dezembro" and `Variável`.str.contains("receita nominal")', engine='python'
    )  # filtra os dados para o mês de dezembro e para as variáveis que contêm "receita nominal"
    data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
    
    # tratamento dos dados principais
    df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
    assert df_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
    df_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
    df_ne = df_ne.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'mean'})  # agrupa por Ano e Variável, somando os valores

    df = data.loc[data['Região'].isin(['Brasil', 'Sergipe']), ['Ano', 'Região', 'Variável', 'Valor']].copy()  # seleciona as colunas relevantes
    df = pd.concat([df, df_ne], ignore_index=True)  # concatena os dados originais com os do Nordeste
    df['Variação'] = df.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual

    df_pivot = pd.pivot(df, index='Ano', columns='Região', values='Variação').reset_index()
    df_pivot.dropna(subset=['Brasil', 'Nordeste', 'Sergipe'], inplace=True)  # remove linhas onde Brasil ou Sergipe são NaN

    df_pivot.to_excel(os.path.join(sheets_path, 'g10.1.xlsx'), index=False, sheet_name='g10.1')

except Exception as e:
    errors['Gráfico 10.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g10.1--g10.2--t10.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
