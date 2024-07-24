import functions as c
import os
import pandas as pd
import numpy as np
import json
import traceback
import tempfile
import shutil


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}

# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

file_names = {}

# VARIÁVEL LEITOS DE INTERNAÇÃO SEM FILTRO
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def'
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click(['/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[2]',
                  '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]'])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

    # abre a tabela
    driver.click('/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    file_names[os.listdir(dbs_path)[0]] = 'LEITOS SEM FILTRO'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (LEITOS DE INTERNAÇÃO SEM FILTRO)'] = traceback.format_exc()


# VARIÁVEL LEITOS DE INTERNAÇÃO NORDESTE
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click(['/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[2]',
                  '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]'])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

    # abre a tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[1]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[1]/select/option[3]'
    ])

    driver.click('/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names[f] = 'LEITOS NORDESTE'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (LEITOS DE INTERNAÇÃO NORDESTE)'] = traceback.format_exc()


# VARIÁVEL LEITOS DE INTERNAÇÃO SERGIPE
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click(['/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[2]',
                  '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]'])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

    # abre a tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[2]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[2]/select[2]/option[27]'
    ])

    driver.click('/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names[f] = 'LEITOS SERGIPE'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (LEITOS DE INTERNAÇÃO SERGIPE)'] = traceback.format_exc()


# VARIÁVEL LEITOS DE INTERNAÇÃO NÃO SUS SEM FILTRO
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click(['/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[3]',
                  '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]'])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')
    driver.click('/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names[f] = 'LEITOS NÃO SUS SEM FILTRO'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (LEITOS NÃO SUS SEM FILTRO)'] = traceback.format_exc()


# VARIÁVEL LEITOS DE INTERNAÇÃO NÃO SUS NORDESTE
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click(['/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[3]',
                  '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]'])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[1]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[1]/select/option[3]'
    ])

    driver.click('/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names[f] = 'LEITOS NÃO SUS NORDESTE'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (LEITOS NÃO SUS NORDESTE)'] = traceback.format_exc()


# VARIÁVEL LEITOS DE INTERNAÇÃO NÃO SUS SERGIPE
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click(['/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[14]',
                  '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[3]',
                  '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]'])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[2]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[2]/select[2]/option[27]'
    ])

    driver.click('/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names[f] = 'LEITOS NÃO SUS SERGIPE'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (LEITOS NÃO SUS SERGIPE)'] = traceback.format_exc()


# VARIÁVEL POPULAÇÃO
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpopuf.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names[f] = 'POPULAÇÃO'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (POPULAÇÃO)'] = traceback.format_exc()

driver.quit()

# ************************
# PLANILHA
# ************************

try:
    dfs = []
    files = []

    # seleciona as bases LEITOS para manipulação
    for k, v in file_names.items():
        if 'LEITOS' in v:
            files.append((v, k))

    for f in files:

        # ignora as primeiras linhas de acordo com a base de dados
        if f[0] == 'LEITOS SEM FILTRO' or f[0] == 'LEITOS NÃO SUS SEM FILTRO' or f[0] == 'POPULAÇÃO':
            df = c.open_file(dbs_path, f[1], 'csv', sep=';', encoding='cp1252', skiprows=3)
        else:
            df = c.open_file(dbs_path, f[1], 'csv', sep=';', encoding='cp1252', skiprows=4)

        cols = df.columns
        # verifica o índice do valor Total, última linha do df
        last_lin = int(df.loc[df[cols[0]] == 'Total'].index.values[0])
        # remove as informações complementares ao final do df
        df = df.iloc[:last_lin + 1]
        df_melted = df.melt(id_vars=cols[0], value_vars=cols[1:], var_name='Ano', value_name='Valor')
        # seleciona apenas o dado referente ao ano
        df_melted['Ano'] = df_melted['Ano'].apply(lambda x: x.split('/')[0])
        # remove informações irrelevantes à frente do dado e filtra as variáveis de interesse
        df_melted[cols[0]] = df_melted[cols[0]].apply(lambda x: ' '.join(x.split()[1:]))
        df_melted = df_melted.loc[df_melted[cols[0]].isin([
            'Administração Pública', 'Entidades Empresariais', 'Entidades sem Fins Lucrativos'])]
        # classifica o df conforme tipo do leito e com a região
        df_melted['Leito'] = 'Não SUS' if 'NÃO SUS' in f[0] else 'SUS'
        df_melted['Região'] = 'Sergipe' if 'SERGIPE' in f[0] else ('Nordeste' if 'NORDESTE' in f[0] else 'Brasil')
        df_melted.sort_values(by=['Região', 'Leito', cols[0], 'Ano'], inplace=True)
        df_melted = df_melted[['Região', 'Leito', cols[0], 'Ano', 'Valor']]
        dfs.append(df_melted)

    df_int = pd.concat(dfs, ignore_index=True)

    # verifica o nome do arquivo da base POPULAÇÃO
    for k, v in file_names.items():
        if v == 'POPULAÇÃO':
            pop_file = k

    # df da variável população
    df_pop = c.open_file(dbs_path, pop_file, 'csv', sep=';', encoding='cp1252', skiprows=3)
    cols = df_pop.columns
    last_lin = df_pop.loc[df_pop[cols[0]] == 'Total'].index.values[0]
    df_pop = df_pop.iloc[:last_lin + 1]

    df_pop_melted = df_pop.melt(cols[0], cols[1:], 'Ano', 'Valor')
    df_pop_melted.sort_values(by=[cols[0], 'Ano'], inplace=True)
    df_pop_melted[cols[0]] = df_pop_melted[cols[0]].str.replace('.. ', '')
    df_pop_filtered = df_pop_melted.loc[df_pop_melted[cols[0]].isin(['Região Nordeste', 'Sergipe', 'Total'])].copy()
    df_pop_filtered.loc[df_pop_filtered[cols[0]] == 'Total', cols[0]] = 'Brasil'
    df_pop_filtered.loc[df_pop_filtered[cols[0]] == 'Região Nordeste', cols[0]] = 'Nordeste'
    df_pop_filtered.rename(columns={cols[0]: 'Região'}, inplace=True)

    # filtragem dos anos a partir de 2012 e até o ano máximo em comum em ambas as tabelas
    min_year = '2012'
    max_year = min(df_int.Ano.unique()[-1], df_pop_filtered.Ano.unique()[-1])
    df_int = df_int.loc[(df_int['Ano'] >= min_year) & (df_int['Ano'] <= max_year)]
    df_pop_filtered = df_pop_filtered.loc[(df_pop_filtered['Ano'] >= min_year) & (df_pop_filtered['Ano'] <= max_year)]

    # união das tabelas para posterior cálculo da taxa
    df_united = df_int.set_index(['Região', 'Ano']).join(df_pop_filtered.set_index(['Região', 'Ano']),
                                                         how='outer', lsuffix='_int', rsuffix='_pop').reset_index()
    df_united['Taxa de internação'] = df_united['Valor_int'].replace(np.nan, 0).astype(float) / (df_united['Valor_pop'] / 100000)
    df_united.drop(['Valor_int', 'Valor_pop'], axis='columns', inplace=True)
    df_united = df_united[['Região', 'Leito', 'Natureza Jurídica', 'Ano', 'Taxa de internação']]
    df_united = df_united.sort_values(by=['Região', 'Leito', 'Ano'])
    df_united[df_united.columns[:3]] = df_united[df_united.columns[:3]].astype('str')
    df_united[df_united.columns[-2]] = pd.to_datetime(df_united[df_united.columns[-2]], format='%Y').dt.strftime('%d/%m/%Y')
    df_united[df_united.columns[-1]] = df_united[df_united.columns[-1]].astype('float64')

    c.to_excel(df_united, sheets_path, 't18.1.xlsx')

except Exception as e:
    errors['Tabela 18.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--t18.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
