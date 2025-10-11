# Módulos do sistema
import os
from dotenv import load_dotenv
from time import sleep

# Módulos de manipulação de dados
import pandas as pd
import numpy as np

# Módulos de scraping
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ************************
# CONSTANTES
# ************************

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
    }

# caminhos
# Navega até o diretório raiz do projeto e define os caminhos para Data/Municipios
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
raw_path = os.path.join(project_root, 'Data', 'Municipios', 'Raw')
mart_path = os.path.join(project_root, 'Data', 'Municipios', 'Mart')
error_path = os.path.join(project_root, 'Doc', 'Municipios')

load_dotenv()
repo_path = 'anuariosocieconomico/T25'
git_token = os.environ.get('GIT_TOKEN')
source_dir = 'VDE'
caged_login = os.environ.get('CAGED_LOGIN')
caged_password = os.environ.get('CAGED_PASSWORD')

# estados do nordeste
ne_states = [
    'Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe'
]


# ************************
# FUNÇÕES
# ************************

# cria uma sessão requests com retries e timeout configuráveis

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
