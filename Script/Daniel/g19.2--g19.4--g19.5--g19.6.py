import functions as c
import os
import json
import pandas as pd
import numpy as np
from time import sleep
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

file_names = []
# DOWNLOAD DA BASE DE DADOS REFERENTE À POPULAÇÃO GERAL ----------------------------------------------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpopuf.def'  # url fonte
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium

try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[3]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleção dos períodos de interesse (necessário filtrar depois, para iniciar a partir de 2012)
    driver.periods('select', 'A', 'option', all_periods=True)

    # checkbox e exibição da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    driver.change_window()  # altera a janela do navegador para a última aberta, contendo a tabela
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')  # download da tabela
    sleep(.5)

    file_names.append(os.listdir(dbs_path)[0])
    c.from_form_to_file(c.open_file(dbs_path, file_names[0], 'csv', encoding='cp1252', sep=';', skiprows=3),
                        dbs_path, 'tabsus-pop-geral.csv')

except:
    errors[url + ' (POPULAÇÃO GERAL)'] = traceback.format_exc()


# DOWNLOAD DA BASE DE DADOS REFERENTE À POPULAÇÃO JOVEM MASCULINA ------------------------------------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpopuf.def'  # url fonte
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[3]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleção dos períodos de interesse (necessário filtrar depois, para iniciar a partir de 2012)
    driver.periods('select', 'A', 'option', all_periods=True)

    # seleção do sexo e faixa etária
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[3]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[3]/select/option[2]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[5]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[6]')

    # checkbox e exibição da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    driver.change_window()  # altera a janela do navegador para a última aberta, contendo a tabela
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')  # download da tabela
    sleep(.5)

    for f in os.listdir(dbs_path):
        if f not in file_names and 'tabsus' not in f:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=5),
                        dbs_path, 'tabsus-pop-jov-masc.csv')

except:
    errors[url + ' (POPULAÇÃO MASC)'] = traceback.format_exc()


# DOWNLOAD DA BASE DE DADOS REFERENTE À POPULAÇÃO JOVEM FEMININA -------------------------------------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpopuf.def'  # url fonte
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[3]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleção dos períodos de interesse (necessário filtrar depois, para iniciar a partir de 2012)
    driver.periods('select', 'A', 'option', all_periods=True)

    # seleção do sexo e faixa etária
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[3]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[3]/select/option[3]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[5]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[6]')

    # checkbox e exibição da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    driver.change_window()  # altera a janela do navegador para a última aberta, contendo a tabela
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')  # download da tabela
    sleep(.5)

    for f in os.listdir(dbs_path):
        if f not in file_names and 'tabsus' not in f:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=5),
                        dbs_path, 'tabsus-pop-jov-fem.csv')

except:
    errors[url + ' (POPULAÇÃO FEM)'] = traceback.format_exc()


# DOWNLOAD DA BASE DE DADOS REFERENTE HOMICÍDIOS POR ARMA DE FOGO (g19.2) ----------------------------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/ext10uf.def'  # url fonte
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[3]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[6]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[277]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[279]')

    # seleção dos períodos de interesse (necessário filtrar depois, para iniciar a partir de 2012)
    driver.periods('select', 'A', 'option', all_periods=True)

    # checkbox e exibição da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    driver.change_window()  # altera a janela do navegador para a última aberta, contendo a tabela
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')  # download da tabela
    sleep(.5)

    for f in os.listdir(dbs_path):
        if f not in file_names and 'tabsus' not in f:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=4),
                        dbs_path, 'tabsus-homicídios-arma-fogo.csv')

except:
    errors[url + ' (HOMICÍDIO ARMA DE FOGO)'] = traceback.format_exc()


# DOWNLOAD DA BASE DE DADOS REFERENTE HOMICÍDIOS DE HOMENS JOVENS (g19.4) ----------------------------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/ext10uf.def'  # url fonte
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[3]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[6]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[269]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[293]')

    # seleção do sexo e da faixa etária
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[10]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[10]/select/option[2]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[6]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[7]')

    # seleção dos períodos de interesse (necessário filtrar depois, para iniciar a partir de 2012)
    driver.periods('select', 'A', 'option', all_periods=True)

    # checkbox e exibição da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    driver.change_window()  # altera a janela do navegador para a última aberta, contendo a tabela
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')  # download da tabela
    sleep(.5)

    for f in os.listdir(dbs_path):
        if f not in file_names and 'tabsus' not in f:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=6),
                        dbs_path, 'tabsus-homicídios-jov-masc.csv')

except:
    errors[url + url + ' (HOMICÍDIO MASC)'] = traceback.format_exc()


# DOWNLOAD DA BASE DE DADOS REFERENTE HOMICÍDIOS DE HOMENS JOVENS POR ARMA DE FOGO (g19.5) -----------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/ext10uf.def'  # url fonte
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[3]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[6]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[277]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[279]')

    # seleção do sexo e da faixa etária
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[10]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[10]/select/option[2]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[6]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[7]')

    # seleção dos períodos de interesse (necessário filtrar depois, para iniciar a partir de 2012)
    driver.periods('select', 'A', 'option', all_periods=True)

    # checkbox e exibição da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    driver.change_window()  # altera a janela do navegador para a última aberta, contendo a tabela
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')  # download da tabela
    sleep(.5)

    for f in os.listdir(dbs_path):
        if f not in file_names and 'tabsus' not in f:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=6),
                        dbs_path, 'tabsus-homicídios-jov-masc-arma-fogo.csv')

except:
    errors[url + ' (HOMICÍDIO MASC ARMA DE FOGO)'] = traceback.format_exc()


# DOWNLOAD DA BASE DE DADOS REFERENTE HOMICÍDIOS DE MULHERES JOVENS (g19.6) -----------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/ext10uf.def'  # url fonte
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[3]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[6]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[269]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[293]')

    # seleção do sexo e da faixa etária
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[10]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[10]/select/option[3]',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[6]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[6]/select[2]/option[7]')

    # seleção dos períodos de interesse (necessário filtrar depois, para iniciar a partir de 2012)
    driver.periods('select', 'A', 'option', all_periods=True)

    # checkbox e exibição da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    driver.change_window()  # altera a janela do navegador para a última aberta, contendo a tabela
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')  # download da tabela
    sleep(.5)

    for f in os.listdir(dbs_path):
        if f not in file_names and 'tabsus' not in f:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=6),
                        dbs_path, 'tabsus-homicídios-jov-fem.csv')

except:
    errors[url + ' (HOMICÍDIO FEM)'] = traceback.format_exc()

driver.quit()

# ************************
# PLANILHA
# ************************


# TRATAMENTO DAS BASES POPULACIONAIS
pop_dbs = [db for db in os.listdir(dbs_path) if 'pop-' in db]  # seleção das bases
for db in pop_dbs:
    try:
        df = c.open_file(dbs_path, db, 'csv')
        lin_threshold = df.loc[df[df.columns[0]] == 'Total'].index.values[0]  # index da última linha de interesse
        df = df.iloc[:lin_threshold].copy()  # exclusão de linhas desnecessárias, como fonte e comentários
        df = df.melt(df.columns[0], df.columns[1:-1], 'Ano', 'Valor').copy()  # melting das colunas
        c.from_form_to_file(df, dbs_path, db)
    except:
        errors[db] = traceback.format_exc()

# TRATAMENTO DAS BASES DE VARIÁVEIS
var_dbs = [db for db in os.listdir(dbs_path) if 'homicídios' in db]  # seleção das bases
for db in var_dbs:
    try:
        df = c.open_file(dbs_path, db, 'csv')
        lin_threshold = df.loc[df[df.columns[0]] == 'Total'].index.values[0]  # index da última linha de interesse
        df = df.iloc[:lin_threshold].copy()  # exclusão de linhas desnecessárias, como fonte e comentários
        df = df.melt(df.columns[0], df.columns[1:-1], 'Ano', 'Valor').copy()  # melting das colunas
        df['Ano'] = df['Ano'].astype(int)  # conversão para int
        df = df.query('Ano >= 2012')  # filtragem a partir de 2012
        maxY = df['Ano'].max()  # para filtragem dos anos da base populacional

        # abertura da base populacional conforme a base de variável
        if db == 'tabsus-homicídios-arma-fogo.csv':
            df_pop = (c.open_file(dbs_path, 'tabsus-pop-geral.csv', 'csv')
                      .query('Ano <= @maxY'))
            var_name = 'Taxa de homicídios por armas de fogo'
            file_name = 'g19.2.xlsx'
        elif db == 'tabsus-homicídios-jov-masc.csv':
            df_pop = (c.open_file(dbs_path, 'tabsus-pop-jov-masc.csv', 'csv')
                      .query('Ano <= @maxY'))
            var_name = 'Taxa de homicídios de homens jovens'
            file_name = 'g19.4.xlsx'
        elif db == 'tabsus-homicídios-jov-masc-arma-fogo.csv':
            df_pop = (c.open_file(dbs_path, 'tabsus-pop-jov-masc.csv', 'csv')
                      .query('Ano <= @maxY'))
            var_name = 'Taxa de homicídios de homens jovens por armas de fogo'
            file_name = 'g19.5.xlsx'
        elif db == 'tabsus-homicídios-jov-fem.csv':
            df_pop = (c.open_file(dbs_path, 'tabsus-pop-jov-fem.csv', 'csv')
                      .query('Ano <= @maxY'))
            var_name = 'Taxa de homicídios de mulheres jovens'
            file_name = 'g19.6.xlsx'

        # união entre as bases; cálculo da taxa
        df_merged = df.merge(df_pop, on=list(df.columns[:2]), how='left', suffixes=('_var', '_pop'), validate='1:1')
        df_merged['Taxa'] = df_merged['Valor_var'].astype(float) / (df_merged['Valor_pop'].astype(float) / 100_000)

        # ranqueamento dos estados por ano
        dfs = []
        for year in df_merged['Ano'].unique():
            temp = df_merged.query('Ano == @year').copy()
            temp['Posição relativamente às demais UF'] = temp['Taxa'].rank(ascending=False)
            dfs.append(temp)

        # união dos dfs; remoção dos códigos que vêm junto ao nome do estado
        df_concat = pd.concat(dfs, ignore_index=True)
        df_concat.loc[:, df_concat.columns[0]] = df_concat[df_concat.columns[0]].apply(lambda x: x[3:])

        # filtragem dos estados nordestinos; agregação dos dados por ano
        estados_nordeste = ['Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí',
                            'Rio Grande do Norte', 'Sergipe']
        df_ne = df_concat.query('`Unidade da Federação`.isin(@estados_nordeste)').copy()
        df_ne = df_ne.groupby('Ano', as_index=False)['Taxa'].mean()
        df_ne['Região'] = 'Nordeste'
        df_ne['Posição relativamente às demais UF'] = np.nan

        # agregação ao nível nacional; seleção de dados de Sergipe
        df_br = df_concat.groupby('Ano', as_index=False)['Taxa'].mean()
        df_br['Região'] = 'Brasil'
        df_br['Posição relativamente às demais UF'] = np.nan

        df_se = df_concat.query('`Unidade da Federação` == "Sergipe"').copy()
        df_se['Região'] = 'Sergipe'
        df_se = df_se[['Ano', 'Taxa', 'Região', 'Posição relativamente às demais UF']]

        # união dos dfs; definição da variável
        df = pd.concat([df_se, df_ne, df_br], ignore_index=True)
        df['Variável'] = var_name
        df = df[['Região', 'Variável', 'Ano', 'Taxa', 'Posição relativamente às demais UF']].copy()
        df.rename(columns={'Taxa': 'Valor'}, inplace=True)

        df['Ano'] = pd.to_datetime(df['Ano'], format='%Y').dt.strftime('%d/%m/%Y')

        c.from_form_to_file(df, file_name_to_save=file_name)
    except:
        try:
            title = file_name[1:5]
            errors[f'Gráfico {title}'] = traceback.format_exc()
        except:
            errors[f'Arquivo: {db}'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g19.2--g19.4--g19.5--g19.6.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
