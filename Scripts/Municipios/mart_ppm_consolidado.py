import traceback
import functions as c
import os
import pandas as pd
import numpy as np
from datetime import datetime
import requests

# Configuração de caminhos
raw_path = c.raw_path
mart_path = c.mart_path
error_path = c.error_path
ipca_path = os.path.join(raw_path, 'ipca.parquet')
os.makedirs(mart_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

try:

# ========================
# PROCESSAMENTO IPCA
# ========================

    df_ipca = pd.read_parquet(ipca_path)
    df_ipca_annual = df_ipca.loc[df_ipca['Mes'].dt.month == 12].copy()  # filtra apenas dezembro de cada ano
    df_ipca_annual.rename(columns={'Mes': 'Ano'}, inplace=True)  # renomeia para Ano, para facilitar o merge
    df_ipca_annual['Ano'] = df_ipca_annual['Ano'].dt.year.astype('int16')  # mantém apenas o ano

    ano_mais_recente = df_ipca_annual.loc[df_ipca_annual['indice_ipca'].notnull(), 'Ano'].max()  # ano mais recente com índice disponível
    max_ipca = df_ipca_annual.loc[df_ipca_annual['Ano'] == ano_mais_recente, 'indice_ipca'].max()  # índice do ano mais recente
    df_ipca_annual['Inflação'] = np.where(
        (df_ipca_annual['indice_ipca'].notnull()) & (df_ipca_annual['indice_ipca'] != 0),
        max_ipca / df_ipca_annual['indice_ipca'],
        np.nan
    )  # fator de correção para valores reais
    df_ipca_annual.drop(columns=['indice_ipca'], inplace=True)  # remove coluna desnecessária

    print(f'IPCA: {len(df_ipca)} registros')


# ========================
# PROCESSAMENTO AQUICULTURA PPM
# ========================

    area_path = os.path.join(raw_path, 'pesquisa_pecuaria_municipal_aquicultura_ppm', 'raw_pesquisa_pecuaria_municipal_aquicultura_ppm_consolidado.parquet')

    pevs_area = pd.read_parquet(area_path)[['Município', 'Variável', 'Tipo de produto da aquicultura', 'Ano', 'Valor']]
    
    max_year = pevs_area['Ano'].max()  # obtém o ano máximo presente na base
    pevs_area = pevs_area.query('Ano >= @max_year - 4').copy()  # filtra os dados para o ano mais recente e os 4 anos anteriores
    pevs_area['Município'] = pevs_area['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    pevs_area = pevs_area.loc[(pevs_area['Valor'].notnull()) & (pevs_area['Valor'] != 0)]  # remove linhas com valor nulo na área

    # Pivot para ter variáveis como colunas
    pevs_area = pevs_area.pivot(
        index=['Município', 'Tipo de produto da aquicultura', 'Ano'], 
        columns='Variável', 
        values='Valor'
    ).reset_index()

    # Merge com IPCA
    pevs_area = pevs_area.merge(df_ipca_annual, on='Ano', how='left')

    # Calcular valor real da produção
    pevs_area['Valor real da produção'] = pevs_area['Valor da produção'] * pevs_area['Inflação']
    pevs_area = pevs_area.rename(columns={'Valor da produção': 'Valor nominal da produção'})

    # Remover coluna de inflação
    pevs_area.drop(columns=['Inflação'], errors='ignore', inplace=True)

    # Calcular métricas e rankings por grupo
    pevs_area['Valor médio real unitário'] = np.where(
        (pevs_area['Valor real da produção'].notnull()) & (pevs_area['Valor real da produção'] != 0),
        (pevs_area['Valor real da produção'] * 1000) / pevs_area['Produção da aquicultura'],
        np.nan
    )

    # Rankings por produto e ano
    for col in ['Produção da aquicultura', 'Valor nominal da produção', 'Valor real da produção', 'Valor médio real unitário']:
        pevs_area[f'Ranking {col}'] = pevs_area.groupby(['Tipo de produto da aquicultura', 'Ano'])[col].rank(method='min', ascending=False)

    # Limpar códigos dos produtos
    pevs_area['Tipo de produto da aquicultura'] = pevs_area['Tipo de produto da aquicultura'].str.replace(r'^\d+\.?\d* - ', '', regex=True)

    # Formatar data
    pevs_area['Ano'] = '31/12/' + pevs_area['Ano'].astype(str)
    
    
    # Salvar
    output_path = os.path.join(mart_path, 'pesquisa_pecuaria_municipal_aquicultura_consolidado.parquet')
    pevs_area.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    print(f'PEVS_area processado e salvo em: {output_path}')

# ========================
# PROCESSAMENTO ORIGEM ANIMAL PPM
# ========================

    extracao_path = os.path.join(raw_path, 'pesquisa_pecuaria_municipal_origem_animal_ppm', 'raw_pesquisa_pecuaria_municipal_origem_animal_ppm_consolidado.parquet')

    ppm_origem = pd.read_parquet(extracao_path)[['Município', 'Variável', 'Tipo de produto de origem animal', 'Ano', 'Valor']]
    
    max_year = ppm_origem['Ano'].max()  # obtém o ano máximo presente na base
    ppm_origem = ppm_origem.query('Ano >= @max_year - 4').copy()  # filtra os dados para o ano mais recente e os 4 anos anteriores
    ppm_origem['Município'] = ppm_origem['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    ppm_origem = ppm_origem.loc[(ppm_origem['Valor'].notnull()) & (ppm_origem['Valor'] != 0)]  # remove linhas com valor nulo na área

    # Pivot para ter variáveis como colunas
    ppm_origem = ppm_origem.pivot(
        index=['Município', 'Tipo de produto de origem animal', 'Ano'], 
        columns='Variável', 
        values='Valor'
    ).reset_index()

    # Merge com IPCA
    ppm_origem = ppm_origem.merge(df_ipca_annual, on='Ano', how='left')

    # Calcular valor real da produção
    ppm_origem['Valor real da produção'] = ppm_origem['Valor da produção'] * ppm_origem['Inflação']
    ppm_origem = ppm_origem.rename(columns={'Valor da produção': 'Valor nominal da produção'})

    # Remover coluna de inflação
    ppm_origem.drop(columns=['Inflação'], errors='ignore', inplace=True)

    # Calcular métricas e rankings por grupo
    ppm_origem['Valor médio real unitário'] = np.where(
        (ppm_origem['Valor real da produção'].notnull()) & (ppm_origem['Valor real da produção'] != 0),
        ppm_origem['Valor real da produção'] / ppm_origem['Produção de origem animal'],
        np.nan
    )

    # Rankings por produto e ano
    for col in ['Produção de origem animal', 'Valor nominal da produção', 'Valor real da produção', 'Valor médio real unitário']:
        ppm_origem[f'Ranking {col}'] = ppm_origem.groupby(['Tipo de produto de origem animal', 'Ano'])[col].rank(method='min', ascending=False)

    # Limpar códigos dos produtos
    ppm_origem['Tipo de produto de origem animal'] = ppm_origem['Tipo de produto de origem animal'].str.replace(r'^\d+\.?\d* - ', '', regex=True)
    
    """Etapa específica para esta base, remover em outros blocos"""
    # Ajuste da unidade de medida do mel
    ppm_origem['Valor médio real unitário'] = np.where(
        ppm_origem['Tipo de produto de origem animal'] == "Mel de abelha",
        ppm_origem['Valor médio real unitário'] * 1000,
        ppm_origem['Valor médio real unitário']
    )

    # Formatar data
    ppm_origem['Ano'] = '31/12/' + ppm_origem['Ano'].astype(str)

    # Salvar
    output_path = os.path.join(mart_path, 'pesquisa_pecuaria_municipal_origem_animal_consolidado.parquet')
    ppm_origem.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    print(f'PPM_origem processado e salvo em: {output_path}')

# ========================
# PROCESSAMENTO REBANHO PPM
# ========================

    rebanho_path = os.path.join(raw_path, 'pesquisa_pecuaria_municipal_rebanho_ppm', 'raw_pesquisa_pecuaria_municipal_rebanho_ppm_consolidado.parquet')

    ppm_rebanho = pd.read_parquet(rebanho_path)[['Município', 'Variável', 'Tipo de rebanho', 'Ano', 'Valor']]
    
    max_year = ppm_rebanho['Ano'].max()  # obtém o ano máximo presente na base
    ppm_rebanho = ppm_rebanho.query('Ano >= @max_year - 4').copy()  # filtra os dados para o ano mais recente e os 4 anos anteriores
    ppm_rebanho['Município'] = ppm_rebanho['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    ppm_rebanho = ppm_rebanho.loc[(ppm_rebanho['Valor'].notnull()) & (ppm_rebanho['Valor'] != 0)]  # remove linhas com valor nulo
    
    # Renomear coluna principal
    ppm_rebanho = ppm_rebanho.rename(columns={'Valor': 'Efetivo dos rebanhos'})
    
    # Calcular ranking por grupo
    ppm_rebanho['Ranking'] = ppm_rebanho.groupby(['Variável', 'Tipo de rebanho', 'Ano'])['Efetivo dos rebanhos'].rank(method='min', ascending=False)
    
    # Limpar dados desnecessários
    ppm_rebanho.drop(columns=['Variável'], errors='ignore', inplace=True)
    
    # Ordenar dados
    ppm_rebanho = ppm_rebanho.sort_values(['Município', 'Tipo de rebanho', 'Ano'])
    
    # Formatar data
    ppm_rebanho['Ano'] = '31/12/' + ppm_rebanho['Ano'].astype(str)
    
    # Salvar
    output_path = os.path.join(mart_path, 'pesquisa_pecuaria_municipal_rebanho_consolidado.parquet')
    ppm_rebanho.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    print(f'PPM_rebanho processado e salvo em: {output_path}')

# ========================
# PROCESSAMENTO VACAS ORDENHADAS PPM
# ========================

    vacas_path = os.path.join(raw_path, 'pesquisa_pecuaria_municipal_vacas_ordenhadas_ppm', 'raw_pesquisa_pecuaria_municipal_vacas_ordenhadas_ppm_consolidado.parquet')

    ppm_vacas = pd.read_parquet(vacas_path)[['Município', 'Variável', 'Ano', 'Valor']]
    
    max_year = ppm_vacas['Ano'].max()  # obtém o ano máximo presente na base
    ppm_vacas = ppm_vacas.query('Ano >= @max_year - 4').copy()  # filtra os dados para o ano mais recente e os 4 anos anteriores
    ppm_vacas['Município'] = ppm_vacas['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    ppm_vacas = ppm_vacas.loc[(ppm_vacas['Valor'].notnull()) & (ppm_vacas['Valor'] != 0)]  # remove linhas com valor nulo
    
    # Renomear coluna principal
    ppm_vacas = ppm_vacas.rename(columns={'Valor': 'Vacas ordenhadas'})
    
    # Calcular ranking por ano
    ppm_vacas['Ranking'] = ppm_vacas.groupby(['Variável', 'Ano'])['Vacas ordenhadas'].rank(method='min', ascending=False)
    
    # Limpar dados desnecessários
    ppm_vacas.drop(columns=['Variável'], errors='ignore', inplace=True)
    
    # Ordenar dados
    ppm_vacas = ppm_vacas.sort_values(['Município', 'Ano'])
    
    # Formatar data
    ppm_vacas['Ano'] = '31/12/' + ppm_vacas['Ano'].astype(str)
    
    # Salvar
    output_path = os.path.join(mart_path, 'pesquisa_pecuaria_municipal_vacas_ordenhadas_consolidado.parquet')
    ppm_vacas.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    print(f'PPM_vacas processado e salvo em: {output_path}')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_mart_ppm_consolidado.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em mart_ppm_consolidado.py em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao processar o PPM. Verifique o log em "Doc/Municipios/log_mart_ppm_consolidado.txt".')