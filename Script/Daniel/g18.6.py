import functions as c
import os
import pandas as pd
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
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
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/nruf.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver = c.Google(visible=True, rep=dbs_path)  # instância do objeto driver do Selenium
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[7]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[15]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    # abre a tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    driver.quit()

# registro do erro em caso de atualizações
except Exception as e:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

try:
    df = c.open_file(dbs_path, os.listdir(dbs_path)[0], 'csv',
                     sep=';', encoding='cp1252', skiprows=3)

    df_melted = df.melt(id_vars=df.columns[0], value_vars=list(df.columns[1:-1]), var_name='Ano',
                        value_name='Valor')
    df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].str.replace(r'^\.\. ', '', regex=True)

    df_melted = df_melted.loc[df_melted[df_melted.columns[0]].isin(['Total', 'Região Nordeste', 'Sergipe'])]
    df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].replace('Total', 'Brasil')

    df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
    df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]], format='%Y')
    df_melted[df_melted.columns[1]] = df_melted[df_melted.columns[1]].dt.strftime('%d/%m/%Y')
    df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('float64')
    df_melted.sort_values(by=['Região/Unidade da Federação', 'Ano'], ascending=[True, True], inplace=True)

    c.to_excel(df_melted, sheets_path, 'g18.6.xlsx')

except Exception as e:
    errors['Gráfico 18.6'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g18.6.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
