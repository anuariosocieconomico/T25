import functions as c
import os
import pandas as pd
import json
import sidrapy
import traceback
import tempfile
import shutil
from datetime import datetime


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS E PLANILHA
# ************************

# Gráfico 15.1
try:
    # looping de requisições para cada tabela da figura
    data = sidrapy.get_table(
        table_code='6706',
        territorial_level='3',ibge_territorial_code='28',
        variable='8413',
        classifications={'2': '6794', '58': 'all'},
        period="all"
    )

    # remoção da linha 0, dados para serem usados como rótulos das colunas
    # não foram usados porque variam de acordo com a tabela
    # seleção das colunas de interesse
    data.drop(0, axis='index', inplace=True)
    data = data[['D1N', 'D5N', 'D2N', 'V']].copy()
    data['D2N'] = pd.to_datetime(data['D2N'], format='%Y')
    data = data.query('D2N.dt.year >= 2015')
    first_year = data['D2N'].dt.year.min()

    # renomeação das colunas
    # filtragem de dados a partir do ano 2015 e em intevalos de 4 anos
    data.columns = ['Região', 'Variável', 'Ano', 'Valor']
    data = data[data['Ano'].dt.year % 4 == first_year % 4]

    # classificação dos dados
    data['Ano'] = data['Ano'].dt.strftime('%d/%m/%Y')
    data['Valor'] = data['Valor'].astype('float')

    # conversão em arquivo csv
    c.to_excel(data, sheets_path, 'g15.1.xlsx')

except Exception as e:
    errors['Gráfico 15.1'] = traceback.format_exc()


# Planilha de Projeções
try:
    # projeção da taxa
    url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Projecao_da_Populacao'
    response = c.open_url(url)
    df = pd.DataFrame(response.json())

    df = df.loc[df['name'].str.startswith('Projecao'),
                ['name', 'path']].sort_values(by='name', ascending=False).reset_index(drop=True)

    url_to_get = df['path'][0].split('/')[-1]
    response = c.open_url(url + '/' + url_to_get)
    df = pd.DataFrame(response.json())
    url_to_get_pib = df.loc[df['name'].str.endswith('indicadores.xlsx'), 'url'].values[0]

    file = c.open_url(url_to_get_pib)
except:
    errors['Planilha de Projeção'] = traceback.format_exc()

# Gráfico 15.2
try:
    # looping de requisições para cada tabela da figura
    dfs = []
    for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
        data = sidrapy.get_table(
            table_code='3727',
            territorial_level=reg[0],ibge_territorial_code=reg[1],
            variable='all',
            period="all"
        )

        # remoção da linha 0, dados para serem usados como rótulos das colunas
        data.drop(0, axis='index', inplace=True)

        dfs.append(data)

    data = pd.concat(dfs, ignore_index=True)

    # seleção das colunas de interesse
    data = data[['D1N', 'D2N', 'V']].copy()
    data['D2N'] = pd.to_datetime(data['D2N'], format='%Y')
    first_year = data['D2N'].dt.year.min()

    # renomeação das colunas
    # filtragem de dados a partir do ano 2010
    data.columns = ['Região', 'Ano', 'Valor']
    data = data[data['Ano'].dt.year % 4 == first_year % 4]
    data['Projeção'] = False

    # importação das projeções
    df = c.open_file(file_path=file.content, ext='xls', skiprows=6, sheet_name='4) INDICADORES')

    # tratamento da projeção
    df_projec = df.loc[df['LOCAL'].isin(['Brasil', 'Nordeste', 'Sergipe']), ['LOCAL', 'ANO', 'TFT']].copy()
    df_projec['ANO'] = pd.to_datetime(df_projec['ANO'], format='%Y')
    
    # filtram os anos que forem iguais ou maiores que ao ano máximo do df principal e menores ou iguais ao ano em exercício
    # também faz a seleção das datas em intervalos de 4 anos
    # por conta da regra da projeção não ser maior que ano vigente, evitando a seleção de todas as projeções,
    # o ano mais recente selecionado pode ser inferior ao ano atual, o que não é o ideal
    # um novo condicional será adicionado para selecionar uma projeção que seja igual ou superior, até o máximo de 4 anos, ao ano vigente
    # Exemplo do que pode correr:
    # df_projec['ANO'].dt.year <= datetime.today().year retorna 2024 como ano máximo, pois a próxima projeção seria maior que o ano vigente
    df_projec = df_projec[
        (df_projec['ANO'].dt.year >= data['Ano'].dt.year.max()) &
        (df_projec['ANO'].dt.year <= datetime.today().year) &
        (df_projec['ANO'].dt.year % 4 == 0)
    ]

    # condicional para selecionar um ano superior ao ano vigente, caso a filtragem anterior retorne o um ano máximo menor que o ano vigente
    max_projec = df_projec['ANO'].dt.year.max()
    if max_projec < datetime.today().year:
        df_max = df_projec[df_projec['ANO'].dt.year == max_projec].copy()
        df_max['ANO'] = df_max['ANO'] + pd.DateOffset(years=4)
        df_projec = pd.concat([df_projec, df_max], ignore_index=True)

    df_projec.columns = ['Região', 'Ano', 'Valor']
    df_projec['Valor'] = df_projec['Valor'].round(2)
    df_projec['Projeção'] = True

    # filtra o df de projeção para evitar retornar algum ano presente no df principal
    df_final = pd.concat([data, df_projec.query('Ano > @data.Ano.max()')], ignore_index=True)
    df_final.sort_values(by=['Região', 'Ano'], inplace=True)

    # classificação dos dados
    df_final['Ano'] = df_final['Ano'].dt.strftime('%d/%m/%Y')
    df_final['Valor'] = df_final['Valor'].astype('float')

    # conversão em arquivo csv
    c.to_excel(df_final, sheets_path, 'g15.2.xlsx')

except Exception as e:
    errors['Gráfico 15.2'] = traceback.format_exc()


# Gráfico 15.3
try:
    # looping de requisições para cada tabela da figura
    dfs = []
    for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
        data = sidrapy.get_table(
            table_code='3834',
            territorial_level=reg[0],ibge_territorial_code=reg[1],
            variable='all',
            period="all"
        )

        # remoção da linha 0, dados para serem usados como rótulos das colunas
        data.drop(0, axis='index', inplace=True)

        dfs.append(data)

    data = pd.concat(dfs, ignore_index=True)

    # seleção das colunas de interesse
    data = data[['D1N', 'D2N', 'V']].copy()
    data['D2N'] = pd.to_datetime(data['D2N'], format='%Y')
    first_year = data['D2N'].dt.year.min()

    # renomeação das colunas
    # filtragem de dados a partir do ano 2010
    data.columns = ['Região', 'Ano', 'Valor']
    data = data[data['Ano'].dt.year % 4 == first_year % 4]
    data['Projeção'] = False

    # importação das projeções
    df = c.open_file(file_path=file.content, ext='xls', skiprows=6, sheet_name='4) INDICADORES')

    # tratamento da projeção
    df_projec = df.loc[df['LOCAL'].isin(['Brasil', 'Nordeste', 'Sergipe']), ['LOCAL', 'ANO', 'TMI_T']].copy()
    df_projec['ANO'] = pd.to_datetime(df_projec['ANO'], format='%Y')
    
    # filtram os anos que forem iguais ou maiores que ao ano máximo do df principal e menores ou iguais ao ano em exercício
    # também faz a seleção das datas em intervalos de 4 anos
    # por conta da regra da projeção não ser maior que ano vigente, evitando a seleção de todas as projeções,
    # o ano mais recente selecionado pode ser inferior ao ano atual, o que não é o ideal
    # um novo condicional será adicionado para selecionar uma projeção que seja igual ou superior, até o máximo de 4 anos, ao ano vigente
    # Exemplo do que pode correr:
    # df_projec['ANO'].dt.year <= datetime.today().year retorna 2024 como ano máximo, pois a próxima projeção seria maior que o ano vigente
    df_projec = df_projec[
        (df_projec['ANO'].dt.year >= data['Ano'].dt.year.max()) &
        (df_projec['ANO'].dt.year <= datetime.today().year) &
        (df_projec['ANO'].dt.year % 4 == 0)
    ]

    # condicional para selecionar um ano superior ao ano vigente, caso a filtragem anterior retorne o um ano máximo menor que o ano vigente
    max_projec = df_projec['ANO'].dt.year.max()
    if max_projec < datetime.today().year:
        df_max = df_projec[df_projec['ANO'].dt.year == max_projec].copy()
        df_max['ANO'] = df_max['ANO'] + pd.DateOffset(years=4)
        df_projec = pd.concat([df_projec, df_max], ignore_index=True)


    df_projec.columns = ['Região', 'Ano', 'Valor']
    df_projec['Valor'] = df_projec['Valor'].round(2)
    df_projec['Projeção'] = True

    # filtra o df de projeção para evitar retornar algum ano presente no df principal
    df_final = pd.concat([data, df_projec.query('Ano > @data.Ano.max()')], ignore_index=True)
    df_final.sort_values(by=['Região', 'Ano'], inplace=True)

    # classificação dos dados
    df_final['Ano'] = df_final['Ano'].dt.strftime('%d/%m/%Y')
    df_final['Valor'] = df_final['Valor'].astype('float')

    # conversão em arquivo csv
    c.to_excel(df_final, sheets_path, 'g15.3.xlsx')

except Exception as e:
    errors['Gráfico 15.3'] = traceback.format_exc()


# Gráfico 15.4
try:
    # looping de requisições para cada tabela da figura
    dfs = []
    for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
        data = sidrapy.get_table(
            table_code='1174',
            territorial_level=reg[0],ibge_territorial_code=reg[1],
            variable='all',
            period="all"
        )

        # remoção da linha 0, dados para serem usados como rótulos das colunas
        data.drop(0, axis='index', inplace=True)

        dfs.append(data)

    data = pd.concat(dfs, ignore_index=True)

    # seleção das colunas de interesse
    data = data[['D1N', 'D2N', 'V']].copy()
    data['D2N'] = pd.to_datetime(data['D2N'], format='%Y')
    first_year = data['D2N'].dt.year.min()

    # renomeação das colunas
    # filtragem de dados a partir do ano 2010
    data.columns = ['Região', 'Ano', 'Valor']
    data = data[data['Ano'].dt.year % 4 == first_year % 4]
    data['Projeção'] = False

    # importação das projeções
    df = c.open_file(file_path=file.content, ext='xls', skiprows=6, sheet_name='4) INDICADORES')

    # tratamento da projeção
    df_projec = df.loc[df['LOCAL'].isin(['Brasil', 'Nordeste', 'Sergipe']), ['LOCAL', 'ANO', 'e0_T']].copy()
    df_projec['ANO'] = pd.to_datetime(df_projec['ANO'], format='%Y')
    
    # filtram os anos que forem iguais ou maiores que ao ano máximo do df principal e menores ou iguais ao ano em exercício
    # também faz a seleção das datas em intervalos de 4 anos
    # por conta da regra da projeção não ser maior que ano vigente, evitando a seleção de todas as projeções,
    # o ano mais recente selecionado pode ser inferior ao ano atual, o que não é o ideal
    # um novo condicional será adicionado para selecionar uma projeção que seja igual ou superior, até o máximo de 4 anos, ao ano vigente
    # Exemplo do que pode correr:
    # df_projec['ANO'].dt.year <= datetime.today().year retorna 2024 como ano máximo, pois a próxima projeção seria maior que o ano vigente
    df_projec = df_projec[
        (df_projec['ANO'].dt.year >= data['Ano'].dt.year.max()) &
        (df_projec['ANO'].dt.year <= datetime.today().year) &
        (df_projec['ANO'].dt.year % 4 == 0)
    ]

    # condicional para selecionar um ano superior ao ano vigente, caso a filtragem anterior retorne o um ano máximo menor que o ano vigente
    max_projec = df_projec['ANO'].dt.year.max()
    if max_projec < datetime.today().year:
        df_max = df_projec[df_projec['ANO'].dt.year == max_projec].copy()
        df_max['ANO'] = df_max['ANO'] + pd.DateOffset(years=4)
        df_projec = pd.concat([df_projec, df_max], ignore_index=True)


    df_projec.columns = ['Região', 'Ano', 'Valor']
    df_projec['Valor'] = df_projec['Valor'].round(2)
    df_projec['Projeção'] = True

    # filtra o df de projeção para evitar retornar algum ano presente no df principal
    df_final = pd.concat([data, df_projec.query('Ano > @data.Ano.max()')], ignore_index=True)
    df_final.sort_values(by=['Região', 'Ano'], inplace=True)

    # classificação dos dados
    df_final['Ano'] = df_final['Ano'].dt.strftime('%d/%m/%Y')
    df_final['Valor'] = df_final['Valor'].astype('float')

    # conversão em arquivo csv
    c.to_excel(df_final, sheets_path, 'g15.4.xlsx')

except Exception as e:
    errors['Gráfico 15.4'] = traceback.format_exc()



# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g15.1--g15.2--g15.3--g15.4.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
