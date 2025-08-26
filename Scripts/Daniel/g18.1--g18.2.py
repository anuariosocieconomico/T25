import functions as c
import os
import pandas as pd
import json
import traceback
import tempfile
import shutil
import ipeadatapy


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# deflator IPEA IPCA
try:
    data = ipeadatapy.timeseries('PRECOS_IPCAG')
    data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
    c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
except Exception as e:
    errors['IPEA IPCA'] = traceback.format_exc()

# g18.1 e g18.2
# url
url = 'http://siops-asp.datasus.gov.br/cgi/tabcgi.exe?SIOPS/SerHist/ESTADO/indicuf.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/center/center/form/select[1]/option[2]',
        '/html/body/center/center/form/select[2]/option[2]',
        '/html/body/center/center/form/select[3]/option[9]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    # abre a tabela
    driver.click([
        '/html/body/center/center/form/p[3]/table/tbody/tr[1]/td[2]/select/option[1]',
        '/html/body/center/center/form/p[3]/table/tbody/tr[2]/td/p[3]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/center/table[2]/tbody/tr/td[1]/a')

    driver.quit()

# registro do erro em caso de atualizações
except Exception as e:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# g18.1
try:
    df = c.open_file(dbs_path, os.listdir(dbs_path)[0], 'csv', sep=';', encoding='cp1252', skiprows=3)

    cols = df.columns[1:-1]
    df_melted = df.melt(id_vars='UF', value_vars=cols, var_name='Ano', value_name='Valor')
    df_melted.sort_values(by=['UF', 'Ano'], inplace=True)

    df_melted['UF'] = df_melted['UF'].astype('str')
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    df_melted['Valor'] = df_melted['Valor'].astype('float64')

    # tabela ipca
    min_year = df_melted['Ano'].min()
    max_year = df_melted['Ano'].max()
    data_ipca = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    data_ipca.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    data_ipca.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    data_ipca['Index'] = 100.00
    data_ipca['Diff'] = 0.00

    for row in range(1, len(data_ipca)):
        data_ipca.loc[row,'Diff'] = 1 + (data_ipca.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        data_ipca.loc[row, 'Index'] = data_ipca.loc[row - 1, 'Index'] / data_ipca.loc[row, 'Diff']  # calcula o índice de preços

    df_merged = pd.merge(df_melted, data_ipca[['Ano', 'Index']], on='Ano', how='left', validate='m:1')  # mescla as tabelas
    df_merged['Valor'] = (df_merged['Valor'] / df_merged['Index']) * 100  # atualiza o valor pela inflação
    df_merged.loc[df_merged['UF'] == 'Total', 'UF'] = 'Brasil'  # renomeia a unidade da federação Total para Brasil

    df_ne = df_merged.loc[df_merged['UF'].isin(c.ne_states)].copy()
    df_ne.loc[:, 'UF'] = 'Nordeste'  # renomeia a unidade da federação Total para Brasil
    df_ne_mean = df_ne.groupby(['UF', 'Ano'])['Valor'].mean().reset_index()  # calcula a média dos estados do Nordeste

    # tabela gasto do último ano
    df_last_year = df_merged.loc[(df_merged['Ano'] == df_merged['Ano'].max()) & (df_merged['UF'] != 'Brasil')].copy()
    df_last_year['Rank'] = df_last_year['Valor'].rank(ascending=False, method='first')  # calcula o ranking do último ano disponível
    df_last_year_final = pd.concat([
        df_last_year[['UF', 'Valor', 'Rank']],
        df_ne_mean.query('Ano == @max_year')[['UF', 'Valor']],
        df_merged.loc[(df_merged['UF'] == 'Brasil') & (df_merged['Ano'] == max_year), ['UF', 'Valor']]
    ], ignore_index=True)
    df_last_year_final = df_last_year_final.query('Rank <= 6 or UF in ["Nordeste", "Brasil", "Sergipe"]', engine='python').copy()  # filtra apenas os 5 primeiros colocados, Nordeste e Brasil
    df_last_year_final.sort_values(by=['Rank', 'UF'], inplace=True)
    df_last_year_final.rename(columns={'UF': 'Região', 'Valor': 'Gasto', 'Rank': 'Posição'}, inplace=True)

    c.to_excel(df_last_year_final, sheets_path, 'g18.1a.xlsx')

    # tabela gasto médio da série histórica
    df_all = df_merged.query('UF != "Brasil"').groupby(['UF'])['Valor'].mean().reset_index()
    df_all['Posição'] = df_all['Valor'].rank(ascending=False, method='first')  # calcula o ranking do gasto médio

    df_all_final = pd.concat([
        df_all[['UF', 'Valor', 'Posição']],
        df_ne_mean.groupby('UF')['Valor'].mean().reset_index(),
        df_merged.loc[df_merged['UF'] == 'Brasil', ['UF', 'Valor']].groupby('UF').mean().reset_index()
    ], ignore_index=True)
    df_all_final = df_all_final.query('Posição <= 6 or UF in ["Nordeste", "Brasil", "Sergipe"]', engine='python').copy()  # filtra apenas os 5 primeiros colocados, Nordeste e Brasil
    df_all_final.sort_values(by=['Posição', 'UF'], inplace=True)
    df_all_final.rename(columns={'UF': 'Região', 'Valor': f'Média ({min_year}-{max_year})', 'Posição': 'Posição'}, inplace=True)

    c.to_excel(df_all_final, sheets_path, 'g18.1b.xlsx')

except Exception as e:
    errors['Gráfico 18.1'] = traceback.format_exc()


# g18.2
try:
    df = c.open_file(dbs_path, os.listdir(dbs_path)[0], 'csv', sep=';', encoding='cp1252', skiprows=3)
    
    cols = df.columns[1:-1]
    df_melted = df.melt(id_vars='UF', value_vars=cols, var_name='Ano', value_name='Valor')
    df_melted.sort_values(by=['UF', 'Ano'], inplace=True)

    df_melted['UF'] = df_melted['UF'].astype('str')
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    df_melted['Valor'] = df_melted['Valor'].astype('float64')

    # tabela ipca
    min_year = df_melted['Ano'].min()
    max_year = df_melted['Ano'].max()
    data_ipca = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    data_ipca.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    data_ipca.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    data_ipca['Index'] = 100.00
    data_ipca['Diff'] = 0.00

    for row in range(1, len(data_ipca)):
        data_ipca.loc[row,'Diff'] = 1 + (data_ipca.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        data_ipca.loc[row, 'Index'] = data_ipca.loc[row - 1, 'Index'] / data_ipca.loc[row, 'Diff']  # calcula o índice de preços

    df_merged = pd.merge(df_melted, data_ipca[['Ano', 'Index']], on='Ano', how='left', validate='m:1')  # mescla as tabelas
    df_merged['Valor'] = (df_merged['Valor'] / df_merged['Index']) * 100  # atualiza o valor pela inflação
    df_merged.loc[df_merged['UF'] == 'Total', 'UF'] = 'Brasil'  # renomeia a unidade da federação Total para Brasil

    df_ne = df_merged.loc[df_merged['UF'].isin(c.ne_states)].copy()
    df_ne.loc[:, 'UF'] = 'Nordeste'  # renomeia a unidade da federação Total para Brasil
    df_ne_mean = df_ne.groupby(['UF', 'Ano'])['Valor'].mean().reset_index()  # calcula a média dos estados do Nordeste

    df_concat = pd.concat([df_merged.query('UF in ["Sergipe", "Brasil"]'), df_ne_mean], ignore_index=True)
    df_concat.sort_values(by=['UF', 'Ano'], ascending=[False, True], inplace=True)
    df_pivoted = df_concat.pivot(index='Ano', columns='UF', values='Valor').reset_index()

    df_final = df_pivoted[['Ano', 'Sergipe', 'Nordeste', 'Brasil']].copy()
    df_final['Ano'] = '31/12/' + df_final['Ano'].astype('str')

    df_final.to_excel(os.path.join(sheets_path, 'g18.2.xlsx'), index=False, sheet_name='g18.2')

except Exception as e:
    errors['Gráfico 18.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g18.1--g18.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
