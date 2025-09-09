import functions as c
import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sidrapy
import json
import traceback
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = os.path.abspath(os.path.join('Scripts', 'Daniel', 'VDE'))
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}

# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# url
url = 'https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/dados-nacionais-1/base-de-dados-e-notas-metodologicas-dos-gestores-estaduais-sinesp-vde-2022-e-2023'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    # request ao site e extração dos elementos da html
    html = c.open_url(url)
    soup = BeautifulSoup(html.text, 'html.parser')

    # Extrai a tag div com id="content-core"
    content_core = soup.find('div', id='content-core')

    # Extrai todas as tags div com class="column col-md-2" dentro de content_core
    columns = content_core.find_all('div', class_='column col-md-2')

    # Itera pelas colunas para extrair a href
    hrefs = []
    for column in columns:
        # Extrai a tag div com class="cover-banner-tile tile-content"
        tile_content = column.find('div', class_='cover-banner-tile tile-content')
        if tile_content:
            # Extrai a href da tag <a>
            link = tile_content.find('a', href=True)
            if link:
                hrefs.append(link['href'])

    for i, href in enumerate(hrefs):
        for tries in range(5):
            try:
                file = c.open_url(href)
                print(f'Base de dados {i+1} baixada com sucesso!')
                break
            except:
                print(f'Erro ao baixar base de dados {i+1}. Tentativa {tries+1} de 5.')
                if tries  == 4:
                    raise
                sleep(5)
        
        c.to_file(dbs_path, f'VDE{i+1}.xlsx', file.content)

    print('Bases de dados baixadas com sucesso!')
except Exception as e:
    errors[url] = traceback.format_exc()

# ************************
# ELABORAÇÃO DA PLANILHA 
# ************************

try:
    # leitura do conteúdo da base de dados
    dfs = []
    for excel in os.listdir(dbs_path):
        if 'concat' not in excel and 'README' not in excel:
            print(f'A executar arquivo {excel}')
            
            # extração das colunas 
            df = pd.read_excel(
                os.path.join(dbs_path, excel),
                usecols=[0, 2, 3, 7, 8 , 9, 10, 11, 12],
                dtype={0: str, 1: str, 2: str, 3: float, 4: float, 5: float, 6: float, 7: float, 8: float}
            )
            
            cols = df.columns
            df['Ano'] = pd.to_datetime(df[cols[2]], format='%Y-%m-%d %H:%M:%S').dt.year
            df_grouped = df.groupby([cols[0], cols[1], df.columns[-1]], as_index=False)[cols[3:]].sum()  # df.columns[-1] porque a coluna Ano não está presente em cols
            dfs.append(df_grouped)
            print('Pronto!')

    df_concat = pd.concat(dfs, ignore_index=True)  # concatena os dataframes
    
    # salvamento da base de dados concatenada para evitar retrabalho em caso de erros
    c.to_excel(df_concat, dbs_path, 'VDE_concat.xlsx')

    # coleta dados populacionais das UFs (GERAL)
    attempts = 0
    while attempts <= 5:
        try:
            data = sidrapy.get_table(
                table_code='7358',
                territorial_level='3', ibge_territorial_code='all',
                variable='all',
                period="all",
                classifications={'2': '5,6794', '287':'100362', '1933':'all'}
            )
            break
        except:
            attempts += 1

    # tratamento básico para join na tabela de homicídios
    data.drop(0, axis='index', inplace=True)
    data = data[['D1N', 'D6N', 'D4N', 'V']]
    data.columns = ['UF', 'Ano', 'Sexo', 'Valor']
    c.convert_type(data, 'Ano', 'int')
    c.convert_type(data, 'Valor', 'int')

    # pivot da tabela de população para separar os dados por sexo em colunas distintas
    data_pivoted = data.pivot(index=['UF', 'Ano'], columns='Sexo', values='Valor').reset_index()

    # lê a base de dados VDE
    df = pd.read_excel(os.path.join(dbs_path, 'VDE_concat.xlsx'))

    # Dicionário de mapeamento: Nome do estado para sigla
    estado_to_uf = {
        'RO': 'Rondônia',
        'AC': 'Acre',
        'AM': 'Amazonas',
        'RR': 'Roraima',
        'PA': 'Pará',
        'AP': 'Amapá',
        'TO': 'Tocantins',
        'MA': 'Maranhão',
        'PI': 'Piauí',
        'CE': 'Ceará',
        'RN': 'Rio Grande do Norte',
        'PB': 'Paraíba',
        'PE': 'Pernambuco',
        'AL': 'Alagoas',
        'SE': 'Sergipe',
        'BA': 'Bahia',
        'MG': 'Minas Gerais',
        'ES': 'Espírito Santo',
        'RJ': 'Rio de Janeiro',
        'SP': 'São Paulo',
        'PR': 'Paraná',
        'SC': 'Santa Catarina',
        'RS': 'Rio Grande do Sul',
        'MS': 'Mato Grosso do Sul',
        'MT': 'Mato Grosso',
        'GO': 'Goiás',
        'DF': 'Distrito Federal'
    }

    # faz mapeamento entre UF e sigla e merge entre as bases de dados
    df['code'] = df['uf'].map(estado_to_uf)
    df_merged = pd.merge(df, data_pivoted, left_on=['code', 'Ano'], right_on=['UF', 'Ano'], how='left')

    # cálculo da taxa de homicídios por 100 mil habitantes, por UF, por ano e por região
    df_merged['Taxa'] = np.nan
    df_merged.loc[df_merged['evento'] == 'Homicídio doloso', 'Taxa'] = df_merged['total_vitima'] / (df_merged['Total'] / 100_000)
    df_merged.loc[df_merged['evento'].str.lower().str.contains('latrocínio'), 'Taxa'] = df_merged['total_vitima'] / (df_merged['Total'] / 100_000)
    df_merged.loc[df_merged['evento'] == 'Feminicídio', 'Taxa'] = df_merged['feminino'] / (df_merged['Mulheres'] / 100_000)
    df_merged.loc[df_merged['evento'] == 'Estupro', 'Taxa'] = df_merged['feminino'] / (df_merged['Mulheres'] / 100_000)
    df_merged.loc[df_merged['evento'] == 'Furto de veículo', 'Taxa'] = df_merged['total'] / (df_merged['Total'] / 100_000)
    df_merged.loc[df_merged['evento'] == 'Roubo de veículo', 'Taxa'] = df_merged['total'] / (df_merged['Total'] / 100_000)
    df_merged.loc[df_merged['evento'] == 'Roubo de carga', 'Taxa'] = df_merged['total'] / (df_merged['Total'] / 100_000)
    df_merged.loc[df_merged['evento'] == 'Morte no trânsito ou em decorrência dele (exceto homicídio doloso)', 'Taxa'] = df_merged['total_vitima'] / (df_merged['Total'] / 100_000)

    # ranking dos estados por taxa de homicídios por 100 mil habitantes
    dfs_ranked = []
    years_to_exclude = {}
    for event in [
        'Homicídio doloso',
        'latrocínio',
        'Feminicídio',
        'Estupro',
        'Furto de veículo',
        'Roubo de veículo',
        'Roubo de carga',
        'Morte no trânsito ou em decorrência dele (exceto homicídio doloso)'
    ]:
        # filtragem por evento
        if event == 'latrocínio':
            df_event = df_merged[df_merged['evento'].str.lower().str.contains(event)].copy()
        else:
            df_event = df_merged[df_merged['evento'] == event].copy()

        # filtragem por ano
        for year in df_event['Ano'].unique():
            df_year = df_event[df_event['Ano'] == year].copy()

            if len(df_year.uf.unique()) < 27 or 0.000000 in df_year['Taxa'].unique(): # verifica se num determinado ano há estados faltantes ou com total_vitima zerado
                if event not in years_to_exclude:
                    years_to_exclude[event] = [year]
                else:
                    years_to_exclude[event].append(year)
            
            df_year['Ranking'] = df_year['Taxa'].rank(ascending=False)
            
            dfs_ranked.append(df_year)

    # concatena os dataframes rankeados
    df_ranked = pd.concat(dfs_ranked, ignore_index=True)
    
    # agrupamento de dados por ano e região
    df_br = df_ranked.groupby(['Ano', 'evento'], as_index=False)['Taxa'].mean()
    df_ne = df_ranked[df_ranked['UF'].isin(c.ne_states)].copy().groupby(['Ano', 'evento'], as_index=False)['Taxa'].mean()
    df_se = df_ranked[df_ranked['uf'] == "SE"].copy()

    temp_dataframes = []
    for dataframe in [(df_se, 'Sergipe'), (df_br, 'Brasil'), (df_ne, 'Nordeste')]:
        temp_dataframe = dataframe[0].copy()
        temp_dataframe['Região'] = dataframe[1]
        temp_dataframe['Variável'] = temp_dataframe['evento']
        temp_dataframe['Ano'] = pd.to_datetime(temp_dataframe['Ano'], format='%Y').dt.strftime('%d/%m/%Y')
        temp_dataframe['Valor'] = temp_dataframe['Taxa']
        temp_dataframe['Posição relativamente às demais UF'] = dataframe[0]['Ranking'] if dataframe[1] == 'Sergipe' else np.nan
        temp_dataframe = temp_dataframe[['Região', 'Ano', 'Variável', 'Valor', 'Posição relativamente às demais UF']]
        
        temp_dataframes.append(temp_dataframe)

    # une os dataframes de estados e regiões e faz tratamento final para exportação
    df_final = pd.concat(temp_dataframes, ignore_index=True)
    df_final.sort_values(['Região', 'Ano'], inplace=True)

    df_final['Faltam dados para todos os Estados'] = False
    for k, v in years_to_exclude.items():
        if k == 'latrocínio':
            df_final.loc[(df_final['Variável'].str.lower().str.contains(k)) & (df_final['Ano'].apply(lambda x: int(x[-4:])).isin(v)), 'Faltam dados para todos os Estados'] = True
        else:
            df_final.loc[(df_final['Variável'] == k) & (df_final['Ano'].apply(lambda x: int(x[-4:])).isin(v)), 'Faltam dados para todos os Estados'] = True

    # exportação da planilha
    for event in [
        ('Homicídio doloso', 'g19.1'),
        ('latrocínio', 'g19.3'),
        ('Feminicídio', 'g19.7'),
        ('Estupro', 'g19.8'),
        ('Morte no trânsito ou em decorrência dele (exceto homicídio doloso)', 'g19.9'),
        ('Furto de veículo', 'g19.10'),
        ('Roubo de veículo', 'g19.11'),
        ('Roubo de carga', 'g19.12')
    ]:
            
        if event[0] == 'latrocínio':  # método de filtragem disitinto para latrocínio
            df_export = df_final[df_final['Variável'].str.lower().str.contains(event[0])].copy()
        else:
            df_export = df_final[df_final['Variável'] == event[0]].copy()

        c.to_excel(df_export, sheets_path, f'{event[1]}.xlsx')
        print(f'Planilha {event[1]} exportada com sucesso!')

except Exception as e:
    errors['Planilha'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g19.1--g19.3--g19.7--g19.8--g19.8--g19.10--g19.11--g19.12.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))
