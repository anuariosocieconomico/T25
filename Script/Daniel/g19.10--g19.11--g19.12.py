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
# ELABORAÇÃO DAS PLANILHAS COM BASE NO FORMULÁRIO 4
# ************************

try:
    # leitura do conteúdo da base de dados; e extração do dado Ano para agrupamento dos valores
    db_pop = c.get_form(repo, forms, '1').query('Sexo == "Total"')  # base com dados populacionais
    db_pop['Ano'] = pd.to_datetime(db_pop['Mês'], format='%Y-%m-%d').dt.year
    db_pop.drop_duplicates(subset=['Estado', 'Ano'], inplace=True)  # exclusão de dados repetidos de População
    db_pop = db_pop[['Estado', 'Ano', 'População']]

    db = c.get_form(repo, forms, '4')  # base com dados das variáveis de interesse
    db['Ano'] = pd.to_datetime(db['Mês'], format='%Y-%m-%d').dt.year

    # união entre as bases
    # com o drop anterior, a relação entre as bases se tornou do tipo muitos para um (m: 1)
    db_merged = db.merge(db_pop, on=['Estado', 'Ano'], how='left', validate='m:1')
except:
    errors['Formulário 4'] = traceback.format_exc()

try:
    # elaboração da planilha g19.10
    df = db_merged.query('Variável == "Furto de veículo"')
    df_sheet = c.get_sheet(df, ['Estado', 'Região', 'Variável', 'Ano', 'População'], 'Valor', 'sum')
    c.from_form_to_file(df_sheet, 'g19.10.xlsx')
except:
    errors['Gráfico 19.10'] = traceback.format_exc()

try:
    # elaboração da planilha g19.11
    df = db_merged.query('Variável == "Roubo de veículo"')
    df_sheet = c.get_sheet(df, ['Estado', 'Região', 'Variável', 'Ano', 'População'], 'Valor', 'sum')
    c.from_form_to_file(df_sheet, 'g19.11.xlsx')
except:
    errors['Gráfico 19.11'] = traceback.format_exc()

try:
    # elaboração da planilha g19.12
    df = db_merged.query('Variável == "Roubo de carga"')
    df_sheet = c.get_sheet(df, ['Estado', 'Região', 'Variável', 'Ano', 'População'], 'Valor', 'sum')
    c.from_form_to_file(df_sheet, 'g19.12.xlsx')
except:
    errors['Gráfico 19.12'] = traceback.format_exc()

g.close()  # encerramento da conexão com o Github
shutil.rmtree(dbs_path)
