'''
comecei a criar este script seguindo as descrições da documentação, mas percebi que ela estava desatualizada,
já que os valores do novo arquivo não batiam com os valores do arquivo do repositório;
além disso, as demais figuras desse grupo utilizavam da fonte sinesp, enquanto a g19.9 utilizava tabnet
'''


import functions as c
import os
import pandas as pd
import numpy as np
import json
import traceback
import tempfile
import shutil
from datetime import datetime
from time import sleep


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
# DOWNLOAD DA BASE DE DADOS REFERENTE HOMICÍDIOS DE HOMENS JOVENS POR ARMA DE FOGO (g19.5) -----------------------------
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/ext10uf.def'  # url fonte
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',  # linha
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[6]',  # coluna
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',  # conteúdo
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[3]/img',  # abre seleção de grupo cid
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[3]/select/option[2]',  # seleciona grupo cid
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

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=4),
                        dbs_path, 'acidentes_transito.csv')

except:
    errors[url + ' (ACIDENTES DE TRÂNSITO)'] = traceback.format_exc()


url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpop2024uf.def'
try:
    driver.get(url)  # acesso à pagina fonte

    # ações de click para definição das variáveis da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',  # linha
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]', # coluna
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]', # conteúdo
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

    for f in os.listdir(dbs_path):
        if f not in file_names:
            file_names.append(f)

    c.from_form_to_file(c.open_file(dbs_path, file_names[-1], 'csv', encoding='cp1252', sep=';', skiprows=3),
                        dbs_path, 'pop.csv')
    
    driver.quit()  # encerra o driver do selenium

except:
    errors[url + ' (POPULAÇÃO GERAL)'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# gráfico 19.9
try:
    # tratamento dados
    dfs = []
    for base in ['acidentes_transito.csv', 'pop.csv']:  # tratamento similar para as duas bases
        data = c.open_file(dbs_path, base, 'csv')
        index = data.loc[data[data.columns[0]] == 'Total'].index.tolist()[0]  # Total é a última linha de dados antes de observações adicionais
        df = data.iloc[:index + 1]  # mantém a linha até total
        cols = df.columns.tolist()
        df[cols[0]] = df[cols[0]].str.replace('..', '').str.strip()  # limpeza da primeira coluna

        df_melted = df.melt(id_vars=cols[0], var_name='Ano', value_name='Valor')
        df_melted['Ano'] = pd.to_numeric(df_melted['Ano'], errors='coerce')
        df_melted['Valor'] = pd.to_numeric(df_melted['Valor'], errors='coerce')
        df_melted.dropna(how='any', inplace=True)
        df_melted.rename(columns={cols[0]: 'UF'}, inplace=True)
        df_melted.loc[df_melted['UF'].str.lower() == 'total', 'UF'] = 'Brasil' # padroniza o nome do Brasil

        dfs.append(df_melted)

    df_merged = pd.merge(dfs[0], dfs[1], on=['UF', 'Ano'], how='inner', suffixes=('_acidentes', '_pop'), validate='1:1')
    df_merged['Pop'] = df_merged['Valor_pop'] / 100_000  # ajuste para população em 100 mil habitantes
    df_merged['Taxa'] = df_merged['Valor_acidentes'] / df_merged['Pop'] # cálculo da taxa

    # ranqueamento dos estados
    df_states = df_merged[(df_merged['UF'] != 'Brasil') & (~df_merged['UF'].str.lower().str.contains('região'))]  # dados apenas dos estados
    df_states['Rank'] = df_states.groupby('Ano')['Taxa'].rank(method='first', ascending=False)
    df_states.sort_values(by=['Ano', 'Rank'], inplace=True)

    df_final = pd.concat(
        [
            df_merged[(df_merged['UF'] == 'Brasil') | (df_merged['UF'].str.lower().str.contains('nordeste'))][['UF', 'Ano', 'Taxa']],
            df_states.query('UF == "Sergipe"')[['UF', 'Ano', 'Taxa', 'Rank']]
        ],
        ignore_index=True
    )
    df_final = df_final.query('Ano >= 2015')
    df_final.rename(columns={'UF': 'Região', 'Taxa': 'Valor', 'Rank': 'Posição relativamente às demais UF'}, inplace=True)
    df_final['Variável'] = 'Morte no trânsito ou em decorrência dele (exceto homicídio doloso)'
    df_final['Ano'] = '01/01/' + df_final['Ano'].astype(int).astype(str)
    df_final['Região'] = df_final['Região'].str.split(' ').str[-1]
    df_final.sort_values(by=['Região', 'Ano'], inplace=True)
    df_final = df_final[['Região', 'Ano', 'Variável', 'Valor', 'Posição relativamente às demais UF']]

    c.to_excel(df_final, sheets_path, 'g19.9.xlsx')
    

except Exception as e:
    errors['Gráfico 19.9'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g19.9.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
