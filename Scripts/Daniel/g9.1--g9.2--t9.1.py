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

# # sidra 3416
# url = 'https://apisidra.ibge.gov.br/values/t/3416/n1/all/n3/all/v/564/p/all/c11046/40312/d/v564%201?formato=json'
# try:
#     data = c.open_url(url)
#     df = pd.DataFrame(data.json())
#     df = df[['D3C', 'D1N', 'D2N', 'V']].copy()
#     df.columns = ['Ano', 'Região', 'Variável', 'Valor']
#     df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
#     df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
#     df['Data'] = pd.to_datetime(df['Ano'], format='%Y%m')  # converte a coluna Ano para datetime
#     df['Ano'] = df['Data'].dt.year  # extrai o ano da coluna Data
#     df['Mês'] = df['Data'].dt.month  # extrai o mês da coluna Data

#     c.to_excel(df, dbs_path, 'sidra_3416.xlsx')
# except Exception as e:
#     errors['Sidra 3416'] = traceback.format_exc()


# sidra 3417
url = 'https://apisidra.ibge.gov.br/values/t/3417/n1/all/n3/all/v/1186/p/all/c11046/40312/d/v1186%201?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3C', 'D1N', 'D2N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Data'] = pd.to_datetime(df['Ano'], format='%Y%m')  # converte a coluna Ano para datetime
    df['Ano'] = df['Data'].dt.year  # extrai o ano da coluna Data
    df['Mês'] = df['Data'].dt.month  # extrai o mês da coluna Data

    c.to_excel(df, dbs_path, 'sidra_3417.xlsx')
except Exception as e:
    errors['Sidra 3417'] = traceback.format_exc()


# # deflator IPEA IPA DI
# try:
#     data = ipeadatapy.timeseries('IGP_IPA')
#     c.to_excel(data, dbs_path, 'ipeadata_ipa_di.xlsx')
# except Exception as e:
#     errors['IPEA IPA DI'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# # gráfico 9.1
# try:
#     data = c.open_file(dbs_path, 'sidra_3416.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= 2009 and `Mês` == 12', engine='python')
#     data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
    
#     df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
#     assert df_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
#     df_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
#     df_ne = df_ne.groupby(['Ano', 'Região', 'Variável', 'Data', 'Mês'], as_index=False).agg({'Valor': 'mean'})  # agrupa por Ano e Variável, somando os valores

#     df = pd.concat([data.query('`Região` in ["Brasil", "Sergipe"]', engine='python'), df_ne], ignore_index=True)  # concatena os dados originais com os do Nordeste
#     df['Variação'] = df.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual
#     df.dropna(subset=['Variação'], inplace=True)  # remove linhas com valores NaN na coluna Variação
#     df['Variável'] = df['Variável'] + ' - Variação mensal (base: igual mês do ano anterior)'  # renomeia a coluna Variável
#     df['Data'] = df['Data'].dt.strftime('%d/%m/%Y')  # formata a coluna Data para ano-mês

#     df_final = df[['Região', 'Variável', 'Data', 'Variação']].copy()  # seleciona as colunas relevantes
#     df_final.rename(columns={'Data': 'Ano', 'Variação': 'Valor'}, inplace=True)  # renomeia as colunas
#     df_final.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano

#     df_final.to_excel(os.path.join(sheets_path, 'g9.1.xlsx'), index=False)

# except Exception as e:
#     errors['Gráfico 9.1'] = traceback.format_exc()


# gráfico 9.2
try:
    data = c.open_file(dbs_path, 'sidra_3417.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= 2009 and `Mês` == 12', engine='python')
    data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
    
    df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
    assert df_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
    df_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
    df_ne = df_ne.groupby(['Ano', 'Região', 'Variável', 'Data', 'Mês'], as_index=False).agg({'Valor': 'mean'})  # agrupa por Ano e Variável, somando os valores

    df = pd.concat([data.query('`Região` in ["Brasil", "Sergipe"]', engine='python'), df_ne], ignore_index=True)  # concatena os dados originais com os do Nordeste
    df['Variação'] = df.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual
    df.dropna(subset=['Variação'], inplace=True)  # remove linhas com valores NaN na coluna Variação
    df['Variável'] = df['Variável'] + ' - Variação mensal (base: igual mês do ano anterior)'  # renomeia a coluna Variável
    df['Data'] = df['Data'].dt.strftime('%d/%m/%Y')  # formata a coluna Data para ano-mês

    df_final = df[['Região', 'Variável', 'Data', 'Variação']].copy()  # seleciona as colunas relevantes
    df_final.rename(columns={'Data': 'Ano', 'Variação': 'Valor'}, inplace=True)  # renomeia as colunas
    df_final.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano

    df_final.to_excel(os.path.join(sheets_path, 'g9.2.xlsx'), index=False)

except Exception as e:
    errors['Gráfico 9.2'] = traceback.format_exc()





# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g3.1--g3.2--g3.3--g3.4--g3.5--g3.6--g3.7--g3.8--g3.9--g3.10.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
