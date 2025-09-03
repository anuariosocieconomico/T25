import functions as c
import os
import pandas as pd
import json
from bs4 import BeautifulSoup
from io import StringIO
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

# VARIÁVEL ÓBITOS
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/evitb10uf.def'
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[6]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    # abre a tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.wait('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    # extrai a tabela da html (ao baixar diretamente no botão, a tabela retornada era incompleta)
    html = driver.browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    df = pd.read_html(StringIO(str(soup)), skiprows=1, header=0, thousands='.')[0]
    cols = df.columns.values.tolist()  # transforma os índices em lista para encontrar o índice de Total
    df = df.iloc[:-1, :cols.index('Total')].copy()  # remove ruídos da última linha e colunas geradas erroneamente

    for col in df.columns[1:]:
        df[col] = df[col].astype(int)

    # alteração necessária na planilha
    df.loc[:, cols[0]] = df[cols[0]].apply(lambda x: x.split()[-1] if not x.startswith('Região') else x)

    # melting - alongamento do df
    cols = df.columns
    df_melted = df.melt(cols[0], cols[1:], 'Ano', 'Valor').copy()
    c.convert_type(df_melted, 'Valor', 'int')
    c.to_csv(df_melted, dbs_path, 'base1.csv')

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (PRIMEIRA BASE)'] = traceback.format_exc()

# VARIÁVEL POPULAÇÃO
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpopuf.def'

try:
    driver.get(url)  # acessa a página

    # LINHA, COLUNA E CONTEÚDO
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # PERÍODOS
    driver.periods('select', 'A', 'option', all_periods=True)

    # FAIXA ETÁRIA
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[3]'
    ])
    driver.shift_click('/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[16]')

    # BOX E EXIBIÇÃO
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    # DOWNLOAD
    driver.change_window()
    driver.wait('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    # extrai a tabela da html (ao baixar diretamente no botão, a tabela retornada era incompleta)
    html = driver.browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    df = pd.read_html(StringIO(str(soup)), skiprows=1, header=0, thousands='.')[0]
    df = df.iloc[:-1].copy()

    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    cols = df.columns
    df.loc[:, cols[0]] = df[cols[0]].apply(lambda x: x.split()[-1] if not x.startswith('Região') else x)

    df_melted = df.melt(cols[0], cols[1:], 'Ano', 'Valor')
    c.convert_type(df_melted, 'Valor', 'int')
    c.to_csv(df_melted, dbs_path, 'base2.csv')

except Exception as e:
    errors[url + ' (SEGUNDA BASE)'] = traceback.format_exc()

driver.quit()

# ************************
# PLANILHA
# ************************

try:
    dfs = []
    min_year = []
    max_year = []
    for f in ['base1.csv', 'base2.csv']:
        data = c.open_file(dbs_path, f, 'csv')
        filters = ["Sergipe", "Região Nordeste", "TOTAL"]
        df = data.query('`Região/Unidade da Federação`.isin(@filters)').copy()
        df.columns = ['Região', 'Ano', 'Valor']
        df.loc[df['Região'] == 'TOTAL', 'Região'] = 'Brasil'
        df.loc[df['Região'] == 'Região Nordeste', 'Região'] = 'Nordeste'
        df.sort_values(by=['Região', 'Ano'], inplace=True)
        df['Var'] = 'óbitos' if 'base1' in f else 'pop'

        min_year.append(df.Ano.min())
        max_year.append(df.Ano.max())

        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_filtered = df_concat[(df_concat['Ano'] >= max(min_year)) & (df_concat['Ano'] <= min(max_year))].copy()
    df_pivoted = df_filtered.pivot(index=['Região', 'Ano'], columns='Var', values='Valor').reset_index().copy()
    df_pivoted['Taxa de mortes evitáveis'] = (df_pivoted['óbitos'] / df_pivoted['pop']) * 100
    df_pivoted.drop(['pop', 'óbitos'], axis='columns', inplace=True)
    c.convert_type(df_pivoted, 'Ano', 'datetime')

    c.to_excel(df_pivoted, sheets_path, 'g18.8.xlsx')

except Exception as e:
    errors['Gráfico 18.8'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g18.8.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
