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

file_names = {}
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/equipobr.def'
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium

for i in range(3):
    text = 'SEM FILTRO' if i == 0 else ('NORDESTE' if i == 1 else 'SERGIPE')

    # tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
    try:
        driver.get(url)  # acessa a página

        # seleciona as opções da tabela
        driver.click([
            '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[24]',
            '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[13]',
            '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[2]'
        ])

        # seleciona todos os períodos disponíveis
        driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

        if i == 1:
            driver.click([
                '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[1]/img',
                '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[1]/select/option[3]'
            ])
        elif i == 2:
            driver.click([
                '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[2]/img',
                '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[2]/select[2]/option[27]'
            ])

        # abre a tabela
        driver.click([
            '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
            '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
        ])

        # altera para a última aba aberta e baixa o arquivo
        driver.change_window()
        driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

        if i == 0:
            file_names[os.listdir(dbs_path)[0]] = 'SEM FILTRO'
        elif i == 1:
            for f in os.listdir(dbs_path):
                if f not in file_names:
                    file_names[f] = 'NORDESTE'
        else:
            for f in os.listdir(dbs_path):
                if f not in file_names:
                    file_names[f] = 'SERGIPE'

    # registro do erro em caso de atualizações
    except Exception as e:
        errors[url] = traceback.format_exc()


# VARIÁVEL POPULAÇÃO
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpopuf.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names[f] = 'POPULAÇÃO'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (POPULAÇÃO)'] = traceback.format_exc()

driver.quit()

# ************************
# PLANILHA
# ************************

try:
    # verifica os nomes das bases
    dfs = []
    for k, v in file_names.items():
        if v != 'POPULAÇÃO':
            if v == 'SEM FILTRO':
                data = c.open_file(dbs_path, k, 'csv', sep=';', encoding='cp1252', skiprows=3)
            else:
                data = c.open_file(dbs_path, k, 'csv', sep=';', encoding='cp1252', skiprows=4)

            # seleciona apenas as linhas com dados, ignorando as informações adicionais incluídas ao final da tabela
            cols = data.columns
            last_lin = data.loc[data[cols[0]] == 'Total'].index.values[0]
            df = data.iloc[:last_lin + 1]

            # melting da tabela para verticalizar os dados referentes aos anos, limpeza e classificação
            df_melted = df.melt(cols[0], cols[1:], 'Ano', 'Valor')
            df_melted['Ano'] = df_melted['Ano'].apply(lambda z: z.split('/')[0])
            df_melted['Região'] = 'Sergipe' if 'SERGIPE' in v else ('Nordeste' if 'NORDESTE' in v else 'Brasil')

            dfs.append(df_melted)

    # variável equipamentos
    df_eq = pd.concat(dfs, ignore_index=True)
    df_eq.sort_values(by=['Região', 'Ano'], inplace=True)

    for k, v in file_names.items():
        if v == 'POPULAÇÃO':
            pop_file = k

    # variável população
    data = c.open_file(dbs_path, pop_file, 'csv', sep=';', encoding='cp1252', skiprows=3)
    cols = data.columns
    last_lin = data[data[cols[0]] == 'Total'].index.values[0]
    df_pop = data.iloc[:last_lin + 1]
    df_pop_melted = df_pop.melt(cols[0], cols[1:], 'Ano', 'Valor')
    df_pop_melted[cols[0]] = df_pop_melted[cols[0]].str.replace('.. ', '')
    df_pop_filtered = df_pop_melted.loc[df_pop_melted[cols[0]].isin(['Região Nordeste', 'Sergipe', 'Total'])]
    df_pop_filtered.loc[df_pop_filtered[cols[0]] == 'Região Nordeste', cols[0]] = 'Nordeste'
    df_pop_filtered.loc[df_pop_filtered[cols[0]] == 'Total', cols[0]] = 'Brasil'
    df_pop_filtered = df_pop_filtered.rename(columns={cols[0]: 'Região'})
    df_pop_filtered = df_pop_filtered.sort_values(by=['Região', 'Ano'])

    # especificação do período máximo e mínimo em comum entre ambas as tabelas
    min_year = '2012'
    max_year = min(df_eq.Ano.unique()[-1], df_pop_filtered.Ano.unique()[-1])
    df_eq = df_eq.loc[(df_eq['Ano'] >= min_year) & (df_eq['Ano'] <= max_year)]
    df_pop_filtered = df_pop_filtered.loc[(df_pop_filtered['Ano'] >= min_year) & (df_pop_filtered['Ano'] <= max_year)]

    # união das tabelas
    df_united = df_eq.set_index(['Região', 'Ano']).join(
        df_pop_filtered.set_index(['Região', 'Ano']),
        how='outer', lsuffix='_equipamentos', rsuffix='_pop'
    ).reset_index()

    # cálculo da taxa
    df_united['Taxa de uso dos equipamentos'] = df_united['Valor_equipamentos'].astype(float) / (df_united['Valor_pop'].astype(float) / 100000)
    df_united = df_united.drop(['Valor_equipamentos', 'Valor_pop'], axis='columns')
    df_united = df_united[['Região', 'Equipamento selecionado', 'Ano', 'Taxa de uso dos equipamentos']]

    df_united[df_united.columns[:2]] = df_united[df_united.columns[:2]].astype('str')
    df_united[df_united.columns[-2]] = pd.to_datetime(df_united[df_united.columns[-2]], format='%Y')
    df_united[df_united.columns[-2]] = df_united[df_united.columns[-2]].dt.strftime('%d/%m/%Y')
    df_united[df_united.columns[-1]] = df_united[df_united.columns[-1]].astype('float64')

    c.to_excel(df_united, sheets_path, 't18.2.xlsx')

except Exception as e:
    errors['Tabela 18.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--t18.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
