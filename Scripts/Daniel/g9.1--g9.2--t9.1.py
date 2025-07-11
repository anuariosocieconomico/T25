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

# sidra 3416
url = 'https://apisidra.ibge.gov.br/values/t/3416/n1/all/n3/all/v/564/p/all/c11046/40312/d/v564%201?formato=json'
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

    c.to_excel(df, dbs_path, 'sidra_3416.xlsx')
except Exception as e:
    errors['Sidra 3416'] = traceback.format_exc()


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


# sidra 1407
url = 'https://apisidra.ibge.gov.br/values/t/1407/n1/all/v/312,368,503,866/p/all/c12354/all/c11066/90077,90084,106644?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D4N', 'D2N', 'D5N', 'MN', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Variável', 'Categoria', 'Unidade', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Ano'] = df['Ano'].astype(int)  # converte a coluna Ano para inteiro
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df = df.query('`Região` != "Brasil" and not `Região`.str.startswith("Região")', engine='python').copy()  # filtra apenas as linhas com Categoria Total e Unidade R$
    assert df['Região'].nunique() == 27, "Número de regiões diferentes de 27"

    c.to_excel(df, dbs_path, 'sidra_1407.xlsx')
except Exception as e:
    errors['Sidra 1407'] = traceback.format_exc()


# deflator IPEA IPCA
try:
    data = ipeadatapy.timeseries('PRECOS_IPCAG')
    data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
    c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
except Exception as e:
    errors['IPEA IPCA'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# gráfico 9.1
try:
    data = c.open_file(dbs_path, 'sidra_3416.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= 2009 and `Mês` == 12', engine='python')
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

    df_final.to_excel(os.path.join(sheets_path, 'g9.1.xlsx'), index=False)

except Exception as e:
    errors['Gráfico 9.1'] = traceback.format_exc()


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


# tabela 9.1
try:
    data = c.open_file(dbs_path, 'sidra_1407.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= 2010', engine='python')
    deflator = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= 2010', engine='python')
    max_year = data['Ano'].max()
    
    # tratamento do deflator
    df_deflator = deflator.query('Ano <= @max_year', engine='python').copy()  # filtra o deflator para o ano mais recente
    df_deflator.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = df_deflator.loc[row - 1, 'Valor'] / df_deflator.loc[row, 'Valor']  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços


    # filtragem das regiões e variável para cálculo da receita bruta e participação de sergipe nas receitas
    df_receita_br = data.query('`Variável`.str.lower().str.contains("receita bruta")', engine='python').copy()
    df_receita_br.loc[:, 'Região'] = 'Brasil'
    df_receita_br = df_receita_br.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'sum'})  # agrupa por Ano, Região e Variável, somando os valores

    df_receita_ne = data.query('`Região` in @c.ne_states and `Variável`.str.lower().str.contains("receita bruta")', engine='python').copy()
    assert df_receita_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
    df_receita_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
    df_receita_ne = df_receita_ne.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'sum'})  # agrupa por Ano e Variável, somando os valores

    df_receita_se = data.query('`Região` == "Sergipe" and `Variável`.str.lower().str.contains("receita bruta")', engine='python').copy()
    df_receita_se = df_receita_se.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'sum'})  # agrupa por Ano, Região e Variável, somando os valores
    
    df_receita = pd.concat([df_receita_br, df_receita_ne], ignore_index=True)  # concatena os dados de receita bruta
    df_receita = df_receita.merge(df_receita_se, on=['Ano', 'Variável'], how='left', validate='m:1')  # mescla com os dados de sergipe
    df_receita = df_receita.merge(df_deflator[['Ano', 'Index']], on='Ano', how='left', validate='m:1')  # mescla com o deflator
    df_receita['Total'] = (df_receita['Valor_x'] / df_receita['Index']) * 100  # ajusta a receita bruta pelo deflator
    df_receita['SE'] = (df_receita['Valor_y'] / df_receita['Index']) * 100  # ajusta a receita bruta de sergipe pelo deflator
    
    # tratamento final da tabela de participação de sergipe na receita bruta
    df_receita['Valor'] = (df_receita['SE'] / df_receita['Total']) * 100  # calcula a participação de sergipe na receita bruta
    df_receita['Indicador'] = 'Participação da receita bruta de Sergipe no ' + df_receita['Região_x']
    df_receita['Unidade'] = '%'
    df_receita_final = df_receita[['Ano', 'Indicador', 'Unidade', 'Valor']].copy()  # seleciona as colunas relevantes

    # soma das receitas brutas de sergipe para cada categoria
    df_receita_categoria = data.query('`Região` == "Sergipe" and `Variável`.str.lower().str.contains("receita bruta")', engine='python').copy()
    df_receita_categoria = df_receita_categoria.groupby(['Ano', 'Categoria'], as_index=False).agg({'Valor': 'sum'})  # agrupa por Ano e Categoria, somando os valores
    df_receita_categoria['Categoria'] = df_receita_categoria['Categoria'].str.split('.').str[-1]  # remove a parte antes do ponto na categoria
    df_receita_categoria['Unidade'] = 'R$ milhões'
    df_receita_categoria.rename(columns={'Categoria': 'Indicador'}, inplace=True)  # renomeia a coluna Categoria para Indicador
    df_receita_categoria.sort_values(['Indicador', 'Ano'], inplace=True)  # ordena os dados por Ano e Indicador
    df_receita_categoria_final = df_receita_categoria[['Ano', 'Indicador', 'Unidade', 'Valor']].copy()  # seleciona as colunas relevantes

    # soma das variáveis de sergipe
    df_variaveis = data.query('`Região` == "Sergipe"', engine='python').copy()
    df_variaveis = df_variaveis.groupby(['Ano', 'Variável'], as_index=False).agg({'Valor': 'sum'})  # agrupa por Ano e Variável, somando os valores
    df_variaveis['Unidade'] = None

    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('pessoal ocupado'), 'Unidade'] = 'Pessoas'
    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('pessoal ocupado'), 'Variável'] = 'Pessoal ocupado em 31/12'

    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('margem de comercialização'), 'Unidade'] = 'R$ milhões'
    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('margem de comercialização'), 'Variável'] = 'Margem de comercialização'

    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('número de unidades'), 'Unidade'] = 'Unidades'
    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('número de unidades'), 'Variável'] = 'Unidades locais'

    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('receita bruta'), 'Unidade'] = 'R$ milhões'
    df_variaveis.loc[df_variaveis['Variável'].str.lower().str.contains('receita bruta'), 'Variável'] = 'Receita bruta de revenda e de comissões sobre venda'

    df_variaveis.sort_values(['Variável', 'Ano'], inplace=True)  # ordena os dados por Ano e Variável
    df_variaveis.rename(columns={'Variável': 'Indicador'}, inplace=True)  # renomeia a coluna Variável para Indicador
    df_variaveis_final = df_variaveis[['Ano', 'Indicador', 'Unidade', 'Valor']].copy()  # seleciona as colunas relevantes

    df_final = pd.concat([df_receita_final, df_receita_categoria_final, df_variaveis_final], ignore_index=True)  # concatena as tabelas


    df_final.to_excel(os.path.join(sheets_path, 't9.1.xlsx'), index=False, sheet_name='t9.1')

except Exception as e:
    errors['Tabela 9.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g3.1--g3.2--g3.3--g3.4--g3.5--g3.6--g3.7--g3.8--g3.9--g3.10.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
