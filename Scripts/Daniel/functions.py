# Módulos do sistema
import os
from dotenv import load_dotenv
from time import sleep
import random
import stat

# Módulos de manipulação de dados
import pandas as pd
import numpy as np
import io
import unicodedata
import zipfile

# Módulos de scraping
import requests
from parsel import Selector
from bs4 import BeautifulSoup

# Módulos de simulação de usuário
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains


# ************************
# CONSTANTES
# ************************

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36(KHTML, like Gecko) '
                         'Chrome/118.0.0.0 Safari/537.36'}

# folders
# script_dir = os.path.dirname(__file__)
# project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
# sheets_dir = os.path.join(project_root, 'Data')
# errors_dir = os.path.join(project_root, 'Doc/relatorios_de_erros')

sheets_dir = os.path.abspath('Data')
errors_dir = os.path.abspath(os.path.join('Doc', 'relatorios_de_erros'))

load_dotenv()
repo_path = 'anuariosocieconomico/T25'
git_token = os.environ.get('GIT_TOKEN')
source_dir = 'VDE'

# estados do nordeste
ne_states = ['Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba',
             'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe']


# ************************
# FUNÇÕES
# ************************

# adiciona certo delay às requisições
def delay_requests(maximum):
    sleep(random.random() * maximum)


# faz requisições aos sites
def open_url(path: str):
    r = requests.get(path, headers=headers)
    delay_requests(2)
    return r


# faz requisições aos site e passa o html para o selector de tags
def get_html(path: str):
    r = requests.get(path, headers=headers)
    sel = Selector(text=r.text)
    delay_requests(2)
    return sel


# abre os arquivos baixados
def open_file(dir_path=None, file_path=None, ext=None, encoding='utf-8',
              sep=',', skiprows=None, excel_name=None, sheet_name=None, decimal=','):

    # se o arquivo for csv
    if ext == 'csv' and file_path is not None:
        if not isinstance(file_path, str):  # verifica se o arquivo está na memória local ou em ‘bytes’
            dataframe = pd.read_csv(io.BytesIO(file_path),
                                    encoding=encoding, sep=sep, decimal=decimal, skiprows=skiprows)
            return dataframe
        else:
            dataframe = pd.read_csv(os.path.join(dir_path, file_path),
                                    encoding=encoding, sep=sep, decimal=decimal, skiprows=skiprows)
            return dataframe

    # se o arquivo for xlsx
    elif ext == 'xls' and file_path is not None:
        if not isinstance(file_path, str):
            dataframe = pd.read_excel(io.BytesIO(file_path),
                                      decimal=decimal, sheet_name=sheet_name, skiprows=skiprows)
            return dataframe
        else:
            dataframe = pd.read_excel(os.path.join(dir_path, file_path),
                                      decimal=decimal, sheet_name=sheet_name, skiprows=skiprows)
            return dataframe

    # se o arquivo for zip
    else:
        if not isinstance(file_path, str) and file_path is not None:
            with zipfile.ZipFile(io.BytesIO(file_path), 'r') as zfile:
                excel_tables = zfile.namelist()
                for e_tb in excel_tables:
                    if e_tb.startswith(excel_name) and (e_tb.endswith('.xlsx') or e_tb.endswith('.xls')):
                        excel_content = zfile.read(e_tb)
            dataframe = pd.read_excel(io.BytesIO(excel_content),
                                      decimal=decimal, sheet_name=sheet_name, skiprows=skiprows)
            return dataframe

        else:
            if file_path is not None:
                with zipfile.ZipFile(os.path.join(dir_path, file_path), 'r') as zfile:
                    excel_tables = zfile.namelist()
                    for e_tb in excel_tables:
                        if e_tb.startswith(excel_name) and (e_tb.endswith('.xlsx') or e_tb.endswith('.xls')):
                            excel_content = zfile.read(e_tb)
                dataframe = pd.read_excel(io.BytesIO(excel_content),
                                          decimal=decimal, sheet_name=sheet_name, skiprows=skiprows)
                return dataframe


# converte dataframes em arquivos csv
def to_csv(data_to_convert, dir_path, data_name, encode='utf-8'):
    data_to_convert.to_csv(os.path.join(dir_path, data_name),
                           encoding=encode, decimal=',', index=False)


# converte dataframes em arquivos xslx
def to_excel(data_to_convert, dir_path, data_name):
    data_to_convert.to_excel(os.path.join(dir_path, data_name), index=False)


# remove acentuação do título do gráfico
def remove_accent(title_txt):
    clean_text = unicodedata.normalize('NFKD', title_txt).encode('ASCII', 'ignore').decode('utf-8')
    return clean_text


def to_file(dir_path, name, content):
    with open(os.path.join(dir_path, name), 'wb') as f:
        f.write(content)


class Google:

    def __init__(self, visible=False, rep=None):
        self.rep_path = rep
        self.options = webdriver.ChromeOptions()
        self.prefs = {'download.default_directory': self.rep_path}
        self.options.add_experimental_option('prefs', self.prefs)
        self.options.add_argument('--allow-running-insecure-content')
        self.options.add_argument('--disable-dev-shm-usage')  # REMOVER EM CASO DE ERRO
        self.options.add_argument('--no-sandbox')  # REMOVER EM CASO DE ERRO
        self.options.add_argument('--remote-debugging-port=9222')  # REMOVER EM CASO DE ERRO

        if not visible:
            self.options.add_argument('--headless=new')

        self.browser = webdriver.Chrome(options=self.options)

    def get(self, url):
        self.browser.get(url)
        self.browser.implicitly_wait(2)

    def click(self, xpath):
        if isinstance(xpath, str):
            button = self.browser.find_element(By.XPATH, xpath)
            ActionChains(self.browser).move_to_element(button).click(button).perform()
            self.browser.implicitly_wait(2)
        else:
            for path in xpath:
                button = self.browser.find_element(By.XPATH, path)
                ActionChains(self.browser).move_to_element(button).click(button).perform()
                self.browser.implicitly_wait(2)

    def shift_click(self, xpath):
        button = self.browser.find_element(By.XPATH, xpath)
        ActionChains(self.browser).key_down(Keys.SHIFT).click(button).key_up(Keys.SHIFT).perform()
        self.browser.implicitly_wait(2)

    def periods(self, table_tag, table_tag_id, periods_tag, all_periods=True, prefix=None):
        html = self.browser.page_source
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find(table_tag, id=table_tag_id)
        periods = table.find_all(periods_tag)

        if all_periods:
            for x in [0, -1]:
                p_name = periods[x].name
                p_attrs = periods[x].attrs
                p_txt = periods[x].text
                path = ''

                for k, v in p_attrs.items():
                    path += f' and @{k}="{v}"'

                path = f'//{p_name}[text()="{p_txt}"' + path + ']'
                p_bttn = self.browser.find_element(By.XPATH, path)
                if x == 0:
                    ActionChains(self.browser).move_to_element(p_bttn).click(p_bttn).perform()
                    self.browser.implicitly_wait(3)
                else:
                    ActionChains(self.browser).key_down(Keys.SHIFT).click(p_bttn).key_up(Keys.SHIFT).perform()
                    self.browser.implicitly_wait(3)
        else:
            dec = [p for p in periods if p.text.startswith(prefix)]
            indexes = [periods.index(p) for p in dec]

            for x, i in enumerate(indexes):
                try:
                    p_bttn = self.browser.find_element(
                        By.XPATH, f'/html/body/div/div/center/div/form/div[3]/div/select/option[{i + 1}]'
                    )
                except:
                    p_bttn = self.browser.find_element(
                        By.XPATH, f'/html/body/table[2]/thead/tr[2]/td[2]/center/form/table['
                                  f'1]/tbody/tr/td/select/option[{i + 1}] '
                    )
                if x == 0:
                    ActionChains(self.browser).move_to_element(p_bttn).click(p_bttn).perform()
                    self.browser.implicitly_wait(1)
                else:
                    ActionChains(self.browser).key_down(Keys.LEFT_CONTROL).click(p_bttn).key_up(
                        Keys.LEFT_CONTROL).perform()
                    self.browser.implicitly_wait(1)

    def change_window(self):
        windows = self.browser.window_handles
        self.browser.switch_to.window(windows[-1])

    def download(self, xpath):
        button = WebDriverWait(self.browser, 300).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        ActionChains(self.browser).move_to_element(button).click(button).perform()
        self.browser.implicitly_wait(3)
        sleep(3)

    def wait(self, xpath):
        WebDriverWait(self.browser, 600).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        self.browser.implicitly_wait(3)

    def random_click(self):
        ActionChains(self.browser).move_by_offset(5, 5).click().perform()

    # def get_file(self):
    #     f = os.listdir(self.temp_dir)[0]
    #     d = self.temp_dir
    #     return d, f

    def get_tag(self, xpath):
        tag = self.browser.find_element(By.XPATH, xpath)
        return tag

    def quit(self):
        self.browser.quit()
        sleep(2)


def convert_type(dataframe, column, dtype):
    if dtype == 'datetime':
        dataframe[column] = pd.to_datetime(dataframe[column], format='%Y')
        dataframe[column] = dataframe[column].dt.strftime('%d/%m/%Y')
    elif dtype == 'float':
        dataframe[column] = dataframe[column].astype('float')
    else:
        dataframe[column] = dataframe[column].astype('int64')


# concede permissão para excluir pasta
def remove_readonly(func, path, excinfo):
    os.chmod(path, stat.S_IWRITE)
    func(path)


# função para abrir o conteúdo de determinado formulário no github
def get_form(repo, forms, form_number):
    for form_path in forms:  # estrutura para encontrar o arquivo especificado na função
        if form_number in form_path:  # comparação
            form_content = repo.get_contents(form_path)  # obtém o arquivo
            data = pd.read_csv(form_content.raw_data['download_url'], encoding='utf-8')  # obtém o conteúdo
            return data


# função para agrupamento de dados e cálculo de taxas das planilhas no github
def get_sheet(database, group_vars, calc_vars, func):
    if func == 'sum':
        df_gp = database.groupby(group_vars, as_index=False)[calc_vars].sum()  # agrupamento de dados
    else:
        df_gp = database.groupby(group_vars, as_index=False)[calc_vars].mean()

    df_gp['Taxa'] = df_gp['Valor'] / (df_gp['População'] / 100_000)  # cálculo da taxa

    temp_dfs = []
    for query_year in df_gp.Ano.unique():  # estrutura para ranqueamento dos Estados por ano
        temp_df = df_gp.query('Ano == @query_year').copy()
        temp_df['Posição relativamente às demais UF'] = temp_df.Taxa.rank(ascending=False)
        temp_dfs.append(temp_df)

    df_ranked = pd.concat(temp_dfs, ignore_index=True)  # compilação dos dados

    # estrutura para obtenção de dados de cada nível regional
    db_se = df_ranked.query('Estado == "Sergipe"').copy()
    db_se['Região'] = 'Sergipe'
    db_se.drop(['População', 'Valor', 'Estado'], axis='columns', inplace=True)
    db_se['Posição relativamente às demais UF'] = db_se['Posição relativamente às demais UF'].astype(int)

    db_ne = (df_ranked.query('Região == "Nordeste"').groupby(['Região', 'Variável', 'Ano'], as_index=False)['Taxa']
             .mean().copy())
    db_ne['Região'] = 'Nordeste'
    db_ne['Posição relativamente às demais UF'] = np.nan

    db_br = df_ranked.groupby(['Variável', 'Ano'], as_index=False)['Taxa'].mean().copy()
    db_br['Região'] = 'Brasil'
    db_br['Posição relativamente às demais UF'] = np.nan
    db_br = db_br[['Região', 'Variável', 'Ano', 'Taxa', 'Posição relativamente às demais UF']]

    df_done = pd.concat([db_se, db_ne, db_br], ignore_index=True)  # compilação dos dados
    df_done.rename(columns={'Taxa': 'Valor'}, inplace=True)  # renomeação da coluna Taxa para Valor

    # transformação de Y para Y-m-d e conversão para d-m-Y
    df_done['Ano'] = pd.to_datetime(df_done['Ano'], format='%Y').dt.strftime('%d-%m-%Y')
    return df_done


# função para conversão de dataframe em planilha
def from_form_to_file(base, repo=None, file_name_to_save=None):
    if repo is not None:
        if file_name_to_save is not None:
            if file_name_to_save.endswith('.csv'):
                base.to_csv(os.path.join(repo, file_name_to_save), encoding='utf-8', index=False)
    else:
        if file_name_to_save is not None:
            if file_name_to_save.endswith('.xlsx'):
                base.to_excel(os.path.join(sheets_dir, file_name_to_save), index=False)
    return

