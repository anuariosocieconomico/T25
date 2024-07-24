import functions as c
import os
import pandas as pd
import numpy as np
import json
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
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
url = 'https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
    driver.get(url)  # acessa a página

    # fecha uma janela dinâmica que aparece ao acessar a página
    login_button = driver.browser.find_element(By.ID, 'govbr-login-overlay-wrapper')
    driver.browser.implicitly_wait(2)
    ActionChains(driver.browser).move_to_element(login_button).click(login_button).perform()
    driver.browser.implicitly_wait(2)

    # fecha a janela de cookies
    ActionChains(driver.browser).move_by_offset(-1, -1).click().perform()
    time.sleep(2)
    try:
        driver.click('/html/body/div[5]/div/div/div/div/div[2]/button[2]')
    finally:
        # procura a url de download do arquivo recém publicado
        url_element = driver.browser.find_element(By.XPATH, '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div['
                                                            '2]/div[1]/div/div[2]/div/div/div/div[2]/ul/li/a[1]')
        url = url_element.get_attribute('href')
        driver.get(url)

        # abre a seção de publicações anteriores
        driver.click('/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[1]/div[1]/div[2]/a')

        # procura a url de download dos arquivos
        url_element = driver.browser.find_element(By.XPATH,
                                                  '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div['
                                                  '2]/div/div[2]/div/div/div/div[2]/ul/li/a')
        url = url_element.get_attribute('href')
        driver.get(url)
        time.sleep(3)

        driver.quit()

# registro do erro em caso de atualizações
except Exception as e:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

try:
    ideb_files = [f for f in os.listdir(dbs_path) if 'ideb' in f]
    ideb_files.sort(reverse=True)

    df = c.open_file(dbs_path, ideb_files[1],
                     'zip', excel_name='divulgacao_regioes_ufs_ideb', skiprows=9)
    dfs_old = []
    for i, tb in enumerate(df.keys()):
        # seleção das linhas e colunas de interesse
        # indicação da série com base na aba aberta
        df_tb = df[tb]
        df_tb = df_tb.loc[df_tb[df_tb.columns[0]] == 'Sergipe', df_tb.iloc[:, [0, 1] + list(
            np.arange(-16, -0))].columns]

        df_tb['Série'] = 'Fundamental - Anos Iniciais' if i == 0 else (
            'Fundamental - Anos Finais' if i == 1 else 'Ensino Médio')

        # renomeação das colunas para indicação do ano a que se refere o dado, para posterior tratamento
        # índice IDEB
        base_year = 2005
        for col in df_tb[df_tb.columns[-17:-9]].columns:
            df_tb.rename(columns={col: 'IDEB ' + str(base_year)}, inplace=True)
            base_year += 2
        # projeções
        base_year = 2007
        for col in df_tb[df_tb.columns[-9:-1]].columns:
            df_tb.rename(columns={col: 'Projeção ' + str(base_year)}, inplace=True)
            base_year += 2

        # reorganização da tabela
        df_melted = pd.melt(df_tb, id_vars=list(df_tb.iloc[:, [0, 1, -1]].columns),
                            value_vars=list(df_tb.columns[2:-1]), var_name='var', value_name='val')
        df_melted['Classe'] = df_melted['var'].apply(lambda x: x[:-5])
        df_melted['Ano'] = df_melted['var'].apply(lambda x: x[-4:])
        df_melted.drop('var', axis='columns', inplace=True)

        # renomeação das colunas
        # reordenação das colunas
        df_melted.columns = ['Região', 'Rede', 'Série', 'Valor', 'Classe', 'Ano']
        df_melted = df_melted[['Ano', 'Região', 'Série', 'Rede', 'Classe', 'Valor']]

        dfs_old.append(df_melted)

    df_old = pd.concat(dfs_old, ignore_index=True)
    year = df_old['Ano'].to_list()
    year.sort(reverse=True)

    df = c.open_file(dbs_path, ideb_files[0],
                     'zip', excel_name='divulgacao_regioes_ufs_ideb', skiprows=9)
    dfs_last = []
    for i, tb in enumerate(df.keys()):
        # seleção das linhas e colunas de interesse
        df_tb = df[tb]
        df_tb = df_tb.loc[df_tb[df_tb.columns[0]] == 'Sergipe',
                          df_tb.iloc[:, [0, 1, -1]].columns]

        # indicação da série com base na aba aberta
        # indicação do ano com base no valor da última publicação
        # indicação do tipo do dado; as últimas publicações dispõem apenas de dados, não há projeções
        df_tb['Série'] = 'Fundamental - Anos Iniciais' if i == 0 else (
            'Fundamental - Anos Finais' if i == 1 else 'Ensino Médio')
        df_tb['Ano'] = str(int(year[0]) + 2)
        df_tb['Classe'] = 'IDEB'

        # renomeação das colunas
        # reordenação das colunas
        df_tb.columns = ['Região', 'Rede', 'Valor', 'Série', 'Ano', 'Classe']
        df_tb = df_tb[['Ano', 'Região', 'Série', 'Rede', 'Classe', 'Valor']]

        dfs_last.append(df_tb)

    df_last = pd.concat(dfs_last, ignore_index=True)

    # união de todas as publicações
    df_united = pd.concat([df_last, df_old], ignore_index=True)
    df_united.sort_values(by=['Ano', 'Região', 'Classe', 'Série'], ascending=[True] * 4, inplace=True)

    df_united['Rede'] = df_united['Rede'].apply(lambda x: x.split()[0])

    # classificação dos dados
    df_united[df_united.columns[0]] = pd.to_datetime(df_united[df_united.columns[0]], format='%Y')
    df_united[df_united.columns[0]] = df_united[df_united.columns[0]].dt.strftime('%d/%m/%Y')
    df_united[df_united.columns[1:-1]] = df_united[df_united.columns[1:-1]].astype('str')
    df_united[df_united.columns[-1]] = df_united[df_united.columns[-1]].astype('float64')

    # conversão em arquivo xlsx
    c.to_excel(df_united, sheets_path, 't17.2.xlsx')

except Exception as e:
    errors['Tabela 17.2'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--t17.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
