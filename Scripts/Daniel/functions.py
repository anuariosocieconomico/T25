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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from parsel import Selector
from bs4 import BeautifulSoup

# Módulos de simulação de usuário
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import Select


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
caged_login = os.environ.get('CAGED_LOGIN')
caged_password = os.environ.get('CAGED_PASSWORD')

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
def open_file(dir_path=None, file_path=None, ext=None, encoding='utf-8', encoding_errors='replace',
              sep=',', skiprows=None, excel_name=None, sheet_name=None, decimal=','):

    # se o arquivo for csv
    if ext == 'csv' and file_path is not None:
        if not isinstance(file_path, str):  # verifica se o arquivo está na memória local ou em ‘bytes’
            dataframe = pd.read_csv(io.BytesIO(file_path),
                                    encoding=encoding, encoding_errors=encoding_errors, sep=sep, decimal=decimal, skiprows=skiprows)
            return dataframe
        else:
            dataframe = pd.read_csv(os.path.join(dir_path, file_path),
                                    encoding=encoding, encoding_errors=encoding_errors, sep=sep, decimal=decimal, skiprows=skiprows)
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
        """
        Inicializa a instância do navegador Chrome com opções configuráveis.

        Args:
            visible (bool, optional): Se False, executa o navegador em modo headless.
                Padrão: False.
            rep (str, optional): Diretório para downloads. Se None, usa o diretório padrão.
        
        Notas:
            As opções '--disable-dev-shm-usage', '--no-sandbox' e '--remote-debugging-port=9222'
            são incluídas para compatibilidade com alguns ambientes. Podem ser removidas se causarem conflitos.
        """
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
        """
        Navega para a URL especificada e aguarda 2 segundos implícitos.

        Args:
            url (str): URL para navegação.
        """
        self.browser.get(url)
        self.browser.implicitly_wait(2)

    def click(self, xpath):
        """
        Clique em um ou múltiplos elementos identificados por XPath.

        Args:
            xpath (Union[str, list]): XPath do elemento ou lista de XPaths.

        Raises:
            NoSuchElementException: Se o elemento não for encontrado.
        """
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
        """
        Clique com a tecla SHIFT pressionada (útil para abrir links em nova janela).

        Args:
            xpath (str): XPath do elemento.

        Raises:
            NoSuchElementException: Se o elemento não for encontrado.
        """
        button = self.browser.find_element(By.XPATH, xpath)
        ActionChains(self.browser).key_down(Keys.SHIFT).click(button).key_up(Keys.SHIFT).perform()
        self.browser.implicitly_wait(2)

    def periods(self, table_tag, table_tag_id, periods_tag, all_periods=True, prefix=None):
        """
        Seleciona períodos em uma tabela HTML (comum em interfaces de seleção de datas).

        Args:
            table_tag (str): Tag HTML da tabela (ex: 'table').
            table_tag_id (str): ID da tabela.
            periods_tag (str): Tag dos elementos de período (ex: 'option').
            all_periods (bool, optional): Se True, seleciona o primeiro e último período.
                Se False, usa 'prefix' para filtrar. Padrão: True.
            prefix (str, optional): Prefixo de texto para filtrar períodos (ex: 'Dez/2023').

        Raises:
            ElementNotInteractableException: Se o período não puder ser clicado.
        """
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
        """Muda o foco para a última janela aberta."""
        windows = self.browser.window_handles
        self.browser.switch_to.window(windows[-1])

    def download(self, xpath):
        """
        Aciona o download clicando em um elemento e aguarda até 5 minutos pela ação.

        Args:
            xpath (str): XPath do botão de download.

        Raises:
            TimeoutException: Se o elemento não aparecer em 5 minutos.
        """
        button = WebDriverWait(self.browser, 300).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        ActionChains(self.browser).move_to_element(button).click(button).perform()
        self.browser.implicitly_wait(3)
        sleep(3)

    def wait(self, xpath):
        """
        Aguarda até 10 minutos pela presença de um elemento.

        Args:
            xpath (str): XPath do elemento esperado.

        Raises:
            TimeoutException: Se o elemento não aparecer no tempo especificado.
        """
        WebDriverWait(self.browser, 180).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        self.browser.implicitly_wait(3)

    def random_click(self):
        """Clique em uma posição aleatória (5px,5px) para desfocar elementos."""
        ActionChains(self.browser).move_by_offset(5, 5).click().perform()

    def switch_iframe(self, iframe_id_name):
        """
        Alterna o foco para o iframe especificado.

        Args:
            iframe_id (str): ID ou name do iframe para o qual deseja-se mudar o foco.
        """
        self.browser.switch_to.frame(iframe_id_name)

    def switch_to_default_content(self):
        """
        Alterna o foco para o conteúdo padrão da página, se tiver mudado o foco para um iframe.
        Essa ação é necessária sempre que um iframe for selecionado.
        """
        self.browser.switch_to.default_content()

    def select(self, element, value):
        """
        Seleciona um valor em um elemento de select.

        Args:
            element (WebElement): Elemento de select que terá o valor selecionado. Deve ser selecionado a partir da função get_tag e o elemento deve ser da tag select.
            value (str): Valor (value) a ser selecionado.

        Returns:
            None
        """
        select = Select(element)
        try:
            select.deselect_all()
        except:
            pass
        select.select_by_value(value)

    def select_with_shift(self, element, first_value, last_value, exdedent):
        """
        Seleciona um range de valores em um elemento de select.

        Args:
            element (WebElement): Elemento de select que terá o valor selecionado. Deve ser selecionado a partir da função get_tag e o elemento deve ser da tag select.
            first_value (WebElement): Primeiro valor a ser selecionado. Dever ser selecionado a partir da função get_tag.
            last_value (WebElement): Último valor a ser selecionado. Dever ser selecionado a partir da função get_tag.

        Returns:
            None
        """
        actions = ActionChains(self.browser)
        select = Select(element)
        select.deselect_all()

        actions.click(first_value).perform()

        actions.move_to_element(exdedent).perform()
        actions.key_down(Keys.SHIFT).click(last_value).key_up(Keys.SHIFT).perform()

    # def get_file(self):
    #     f = os.listdir(self.temp_dir)[0]
    #     d = self.temp_dir
    #     return d, f

    def get_tag(self, xpath):
        """
        Retorna um elemento web com base no XPath.

        Args:
            xpath (str): XPath do elemento.

        Returns:
            WebElement: Elemento encontrado.
        """
        tag = self.browser.find_element(By.XPATH, xpath)
        return tag

    def quit(self):
        """Fecha o navegador e aguarda 2 segundos."""
        sleep(2)
        self.browser.quit()
        sleep(5)


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

mapping_states_abbreviation = {
    'Brasil': 'BR',
    'Rondônia': 'RO',
    'Acre': 'AC',
    'Amazonas': 'AM',
    'Roraima': 'RR',
    'Pará': 'PA',
    'Amapá': 'AP',
    'Tocantins': 'TO',
    'Nordeste': 'NE',
    'Maranhão': 'MA',
    'Piauí': 'PI',
    'Ceará': 'CE',
    'Rio Grande do Norte': 'RN',
    'Paraíba': 'PB',
    'Pernambuco': 'PE',
    'Alagoas': 'AL',
    'Sergipe': 'SE',
    'Bahia': 'BA',
    'Minas Gerais': 'MG',
    'Espírito Santo': 'ES',
    'Rio de Janeiro': 'RJ',
    'São Paulo': 'SP',
    'Paraná': 'PR',
    'Santa Catarina': 'SC',
    'Rio Grande do Sul': 'RS',
    'Mato Grosso do Sul': 'MS',
    'Mato Grosso': 'MT',
    'Goiás': 'GO',
    'Distrito Federal': 'DF'
}

mapping_states_ibge_code = {
    'Rondônia': 11,
    'Acre': 12,
    'Amazonas': 13,
    'Roraima': 14,
    'Pará': 15,
    'Amapá': 16,
    'Tocantins': 17,
    'Maranhão': 21,
    'Piauí': 22,
    'Ceará': 23,
    'Rio Grande do Norte': 24,
    'Paraíba': 25,
    'Pernambuco': 26,
    'Alagoas': 27,
    'Sergipe': 28,
    'Bahia': 29,
    'Minas Gerais': 31,
    'Espírito Santo': 32,
    'Rio de Janeiro': 33,
    'São Paulo': 35,
    'Paraná': 41,
    'Santa Catarina': 42,
    'Rio Grande do Sul': 43,
    'Mato Grosso do Sul': 50,
    'Mato Grosso': 51,
    'Goiás': 52,
    'Distrito Federal': 53
}


def create_session_with_retries(total_retries=3, backoff_factor=2, timeout=60):
    """
    Cria uma sessão requests com retries e timeout configuráveis.
    
    Args:
        total_retries (int): número máximo de tentativas.
        backoff_factor (float): fator de espera exponencial entre tentativas.
        timeout (int or float): timeout padrão em segundos para cada request.
        
    Returns:
        session (requests.Session): sessão pronta para uso.
    """
    session = requests.Session()
    
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]  # GET é usado para download
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Armazenamos o timeout como atributo customizado para uso fácil
    session.request_timeout = timeout
    return session
