import functions as c
import os
import pandas as pd
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
    df_melted['Ano'] = pd.to_datetime(df_melted['Ano'], format='%Y')
    df_melted['Ano'] = df_melted['Ano'].dt.strftime('%d/%m/%Y')
    df_melted['Valor'] = df_melted['Valor'].astype('float64')

    c.to_excel(df_melted, sheets_path, 'g18.1.xlsx')

except Exception as e:
    errors['Gráfico 18.1'] = traceback.format_exc()

# g18.2
try:
    data = c.open_file(dbs_path, os.listdir(dbs_path)[0], 'csv', sep=';', encoding='cp1252', skiprows=3)

    cols = data.columns
    df_melted = data.melt(cols[0], cols[1:-1], 'Ano', 'Valor').copy()

    df_ne = df_melted.loc[df_melted[cols[0]].isin(c.ne_states)].copy()
    df_ne.loc[:, cols[0]] = 'Nordeste'
    df_ne_sum = df_ne.groupby([cols[0], 'Ano'])['Valor'].mean().reset_index()

    df = pd.concat([
        df_melted.loc[df_melted[cols[0]].isin(['Sergipe', 'Total'])],
        df_ne_sum
    ], ignore_index=True)
    df.columns = ['Região', 'Ano', 'Valor']
    df.loc[df['Região'] == 'Total', 'Região'] = 'Brasil'
    df.sort_values(by=['Região', 'Ano'], inplace=True)

    c.convert_type(df, 'Ano', 'datetime')
    c.convert_type(df, 'Valor', 'float')

    c.to_excel(df, sheets_path, 'g18.2.xlsx')

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
