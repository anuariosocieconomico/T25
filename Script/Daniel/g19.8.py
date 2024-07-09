import functions as c
import os
from github import Github
from github import Auth
import pandas as pd
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
# IMPORTAÇÃO DAS BASES DO GITHUB
# ************************

# inicialização do repositório
auth = Auth.Token(c.git_token)
g = Github(auth=auth)
repo = g.get_repo(c.repo_path)
contents = repo.get_contents(c.source_dir)

# coleta dos formulários
forms = []
for content_file in contents:
    forms.append(content_file.path)


# ************************
# ELABORAÇÃO DAS PLANILHAS COM BASE NO FORMULÁRIO 3
# ************************

try:
    # leitura do conteúdo da base de dados; e extração do dado Ano para agrupamento dos valores
    db = c.get_form(repo, forms, '3')
    db['Ano'] = pd.to_datetime(db['Mês'], format='%Y-%m-%d').dt.year
except:
    errors['Formulário 3'] = traceback.format_exc()

try:
    # elaboração da planilha g19.8
    df = db.query('Variável == "Estupro" & Sexo == "Feminino"')
    df_sheet = c.get_sheet(df, ['Estado', 'Região', 'Variável', 'Ano', 'População'], 'Valor', 'sum')
    c.from_form_to_file(df_sheet, 'g19.8.xlsx')
except:
    errors['Gráfico 19.8'] = traceback.format_exc()

g.close()
shutil.rmtree(dbs_path)
