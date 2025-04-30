import functions as c
import os
import pandas as pd
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
        url_element = driver.browser.find_element(
            By.XPATH, '/html/body/div[2]/div[1]/main/div[2]/div/div[4]/div[2]/div/div/ul[4]/li/a'
        )
        url = url_element.get_attribute('href')
        driver.get(url)
        time.sleep(6)

        driver.quit()

# registro do erro em caso de atualizações
except Exception as e:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

try:
    df = c.open_file(dbs_path, os.listdir(dbs_path)[0], 'zip', excel_name='divulgacao', skiprows=9)

    dfs = []
    for i, tb in enumerate(df.keys()):
        df_tb = df[tb].copy()  # cópia de cada aba da planilha
        uf_col = df_tb.columns[0]  # rótulo da coluna de UFs
        rede_col = df_tb.columns[1]  # rótulo da coluna de Redes

        # seleção das colunas de UF, Rede, IDEB e Projeções
        select_cols = [
            col for col in df_tb.columns if col.startswith('Unnamed:') or col.startswith('VL_OBSERVADO') or col.startswith('VL_PROJECAO')
        ]
        df_tb = df_tb.loc[df_tb[uf_col] == 'Sergipe', select_cols]  # filtragem de linhas e colunas

        # verticalização do df
        df_tb = df_tb.melt(
            id_vars=select_cols[:2], value_vars=select_cols[2:], var_name='Cat', value_name='Val'
        )

        # extração do ano a partir dos valores da categoria (VL_OBSERVADO_2005)
        df_tb['Ano'] = df_tb['Cat'].str.split('_').str[-1]
        # indicação da série
        df_tb['Série'] = 'Fundamental - Anos Iniciais' if i == 0 else (
            'Fundamental - Anos Finais' if i == 1 else 'Ensino Médio'
        )

        # remoção de parênteses de valores de Rede e renomeação das categorias
        df_tb[rede_col] = df_tb[rede_col].str.split(' ').str[0]
        df_tb['Cat'] = df_tb['Cat'].apply(lambda x: 'IDEB' if x.startswith('VL_OBSERVADO') else 'Projeção')

        # renomeação e reordenação das colunas
        df_tb.rename(columns={uf_col: 'Região'}, inplace=True)
        df_tb.rename(columns={rede_col: 'Rede'}, inplace=True)
        df_tb.rename(columns={'Cat': 'Classe'}, inplace=True)
        df_tb.rename(columns={'Val': 'Valor'}, inplace=True)

        df_tb = df_tb[['Ano', 'Região', 'Série', 'Rede', 'Classe', 'Valor']]

        # appeding
        dfs.append(df_tb)

    # unificação das planilhas
    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.sort_values(by=['Ano', 'Classe', 'Série', 'Rede'], inplace=True)
    df_concat.reset_index(drop=True, inplace=True)

    # classificação dos dados
    df_concat['Ano'] = pd.to_datetime(df_concat['Ano'], format='%Y')
    df_concat['Ano'] = df_concat['Ano'].dt.strftime('%d/%m/%Y')
    df_concat['Valor'] = df_concat['Valor'].astype('float')

    # conversão em arquivo xlsx
    c.to_excel(df_concat, sheets_path, 't17.2.xlsx')

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
