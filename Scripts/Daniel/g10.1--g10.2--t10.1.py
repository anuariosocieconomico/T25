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

# # sidra 5906
# url = 'https://apisidra.ibge.gov.br/values/t/5906/n1/all/n3/all/v/7168/p/all/c11046/all/d/v7168%205?formato=json'
# try:
#     data = c.open_url(url)
#     df = pd.DataFrame(data.json())
#     df = df[['D3N', 'D1N', 'D4N', 'V']].copy()
#     df.columns = ['Data', 'Região', 'Variável', 'Valor']
#     df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
#     df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
#     df['Ano'] = df['Data'].str.split(' ').str[-1]  # extrai o ano da coluna Data
#     df['Ano'] = df['Ano'].astype(int)  # converte a coluna Ano para inteiro
#     df['Mês'] = df['Data'].str.split(' ').str[0]  # extrai o mês da coluna Data

#     c.to_excel(df, dbs_path, 'sidra_5906.xlsx')
# except Exception as e:
#     errors['Sidra 5906'] = traceback.format_exc()


# comércio e serviços
try:
    year = datetime.now().year
    while True:
        # url da base contas regionais
        url = f'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Comercio_e_Servicos/Pesquisa_Anual_de_Servicos/pas{year}/xlsx'
        response = c.open_url(url)
        
        if response.status_code == 200:
            content = pd.DataFrame(response.json())
            final_year = str(year)
            link = content.query(
                'name.str.lower().str.contains(@final_year) and name.str.lower().str.endswith(".zip")'
            )['url'].values[0]
            if link:
                response = c.open_url(link)
                c.to_file(dbs_path, 'pesquisa_servicos.zip', response.content)
                break
        else:
            if year > 2020:
                year -= 1
            else:
                break

except Exception as e:
    errors[url + ' (Pesquisa Anual de Servicos)'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# # gráfico 10.1
# try:
#     data = c.open_file(dbs_path, 'sidra_5906.xlsx', 'xls', sheet_name='Sheet1').query(
#         '`Mês` == "dezembro" and `Variável`.str.contains("receita nominal")', engine='python'
#     )  # filtra os dados para o mês de dezembro e para as variáveis que contêm "receita nominal"
#     data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
    
#     # tratamento dos dados principais
#     df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
#     assert df_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
#     df_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
#     df_ne = df_ne.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'mean'})  # agrupa por Ano e Variável, somando os valores

#     df = data.loc[data['Região'].isin(['Brasil', 'Sergipe']), ['Ano', 'Região', 'Variável', 'Valor']].copy()  # seleciona as colunas relevantes
#     df = pd.concat([df, df_ne], ignore_index=True)  # concatena os dados originais com os do Nordeste
#     df['Variação'] = df.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual

#     df_pivot = pd.pivot(df, index='Ano', columns='Região', values='Variação').reset_index()
#     df_pivot.dropna(subset=['Brasil', 'Nordeste', 'Sergipe'], inplace=True)  # remove linhas onde Brasil ou Sergipe são NaN

#     df_pivot.to_excel(os.path.join(sheets_path, 'g10.1.xlsx'), index=False, sheet_name='g10.1')

# except Exception as e:
#     errors['Gráfico 10.1'] = traceback.format_exc()


# # gráfico 10.2
# try:
#     data = c.open_file(dbs_path, 'sidra_5906.xlsx', 'xls', sheet_name='Sheet1').query(
#         '`Mês` == "dezembro" and `Variável`.str.contains("volume de serviços")', engine='python'
#     )  # filtra os dados para o mês de dezembro e para as variáveis que contêm "receita nominal"
#     data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
    
#     # tratamento dos dados principais
#     df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
#     assert df_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
#     df_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
#     df_ne = df_ne.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'mean'})  # agrupa por Ano e Variável, somando os valores

#     df = data.loc[data['Região'].isin(['Brasil', 'Sergipe']), ['Ano', 'Região', 'Variável', 'Valor']].copy()  # seleciona as colunas relevantes
#     df = pd.concat([df, df_ne], ignore_index=True)  # concatena os dados originais com os do Nordeste
#     df['Variação'] = df.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual

#     df_pivot = pd.pivot(df, index='Ano', columns='Região', values='Variação').reset_index()
#     df_pivot.dropna(subset=['Brasil', 'Nordeste', 'Sergipe'], inplace=True)  # remove linhas onde Brasil ou Sergipe são NaN

#     df_pivot.to_excel(os.path.join(sheets_path, 'g10.2.xlsx'), index=False, sheet_name='g10.2')

# except Exception as e:
#     errors['Gráfico 10.2'] = traceback.format_exc()


##########
# A FONTE ESPECIFICADA NA DOCUMENTAÇÃO BAIXA ARQUIVOS ZIP, UM A UM PARA CADA ANO DA SÉRIE HISTÓRICA.
# ISSO NÃO É PRÁTICO PARA O SCRIPT, ENTÃO FOI UTILIZADO O SIDRAPI PARA BAIXAR A BASE DE DADOS
# CONTINUAR SCRIPT COM ESSA FONTE:
# https://sidra.ibge.gov.br/tabela/2715
# JÁ VERIFIQUEI E O SIDRAPI RETORNA OS MESMOS DADOS QUE A PESQUISA ANUAL DE SERVIÇOS (PAS) DO IBGE
##########

# gráfico 10.2
try:
    data = c.open_file(dbs_path, 'pesquisa_servicos.zip', 'zip', excel_name='Tabela 81')
    
    for tb, reg in [('Tab81a', 'Brasil'), ('Tab81e', 'Nordeste'), ('Tab81i', 'Sergipe')]:
        df = data[tb].copy()
        cols = df.columns.tolist()
        df[cols[0]] = df[cols[0]].str.strip()  # remove espaços em branco do início e fim da primeira coluna
        
        if reg in ['Brasil', 'Nordeste']:
            row = df.loc[df[df.columns[0]] == reg].index
            df_row = df.iloc[row[0], [0, 1]].copy()  # obtém a linha correspondente à região e a coluna 1
            df_row['Região'] = reg  # adiciona a coluna Região

        else:
            row_exclude = df.loc[df[df.columns[0]] == 'Bahia'].index
            df_row = df.iloc[:row_exclude[0], [0, 1]]  # remove linhas correspondentes à Bahia (possuem as mesmas variáveis)
            rows_keep = df_row[
                (df_row[df_row.columns[0]].str.contains('Serviços prestados principalmente às famílias')) &
                (df_row[df_row.columns[0]].str.contains('Serviços de informação e comunicação')) &
                (df_row[df_row.columns[0]].str.contains('Serviços profissionais, administrativos e complementares')) &
                (df_row[df_row.columns[0]].str.contains('correio')) &  # há uma quebra de linha na variável, resultando em duas linhas; essa é a que contém o valor
                (df_row[df_row.columns[0]].str.contains('Atividades imobiliárias')) &
                (df_row[df_row.columns[0]].str.contains('Serviços de manutenção e reparação')) &
                (df_row[df_row.columns[0]].str.contains('Outras atividades de serviços'))
            ].index
        df_row = df_row.iloc[rows_keep, :].copy()
        df_row['Região'] = reg


    df_pivot.to_excel(os.path.join(sheets_path, 'g10.2.xlsx'), index=False, sheet_name='g10.2')

except Exception as e:
    errors['Gráfico 10.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g10.1--g10.2--t10.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
