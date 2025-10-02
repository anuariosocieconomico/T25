"""
SCRIPT MART - PRODUÇÃO DA EXTRAÇÃO VEGETAL E DA SILVICULTURA (PEV)

Processa as bases raw de PEV e gera três bases tratadas:
1. PEVS_area - Área da silvicultura
2. PEVS_extracao - Extração vegetal com correção inflacionária
3. PEVS_silvicultura - Silvicultura com correção inflacionária

Baseado no código R de referência.
"""

import functions as c
import os
import pandas as pd
import numpy as np
from datetime import datetime
import requests

# Configuração de caminhos
raw_path = c.raw_path
mart_path = c.mart_path
ipca_path = os.path.join(raw_path, 'ipca.parquet')
os.makedirs(mart_path, exist_ok=True)


# ========================
# PROCESSAMENTO IPCA
# ========================

if os.path.exists(ipca_path):
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
else:
    print('ERRO: Base IPCA não encontrada')


# ========================
# PROCESSAMENTO ÁREA PEV
# ========================

area_path = os.path.join(raw_path, 'producao_extracao_vegetal_silvicultra_area_pev', 'raw_producao_extracao_vegetal_silvicultra_area_pev_consolidado.parquet')

if os.path.exists(area_path):
    pevs_area = pd.read_parquet(area_path)[['Município', 'Variável', 'Espécie florestal', 'Ano', 'Valor', 'Unidade de Medida']]
    
    max_year = pevs_area['Ano'].max()  # obtém o ano máximo presente na base
    pevs_area = pevs_area.query('Ano >= @max_year - 4').copy()  # filtra os dados para o ano mais recente e os 4 anos anteriores
    pevs_area['Município'] = pevs_area['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    pevs_area = pevs_area.rename(columns={'Valor': 'Área total da silvicultura'})  # renomeia coluna
    pevs_area = pevs_area.sort_values(['Município', 'Espécie florestal', 'Ano'])  # ordena os dados
    pevs_area['Ranking'] = pevs_area.groupby(['Variável', 'Espécie florestal', 'Ano'])['Área total da silvicultura'].rank(method='min', ascending=False)  # cria o ranking
    pevs_area['Ano'] = '31/12/' + pevs_area['Ano'].astype(str)  # formata o ano como data
    pevs_area.drop(columns=['Variável', 'Unidade de Medida'], errors='ignore', inplace=True)  # remove coluna desnecessária
    pevs_area = pevs_area.loc[(pevs_area['Área total da silvicultura'].notnull()) & (pevs_area['Área total da silvicultura'] != 0)]  # remove linhas com valor nulo na área
    
    # Salvar
    output_path = os.path.join(mart_path, 'producao_extracao_vegetal_silvicultra_area_consolidado.parquet')
    pevs_area.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    print(f'PEVS_area processado e salvo em: {output_path}')

# ========================
# PROCESSAMENTO EXTRAÇÃO PEV
# ========================

extracao_path = os.path.join(raw_path, 'producao_extracao_vegetal_silvicultra_extracao_pev', 'raw_producao_extracao_vegetal_silvicultra_extracao_pev_consolidado.parquet')

if os.path.exists(extracao_path):
    pevs_extracao = pd.read_parquet(extracao_path)[['Município', 'Variável', 'Tipo de produto extrativo', 'Ano', 'Valor']]

    max_year = pevs_extracao['Ano'].max()  # obtém o ano máximo presente na base
    pevs_extracao = pevs_extracao.query('Ano >= @max_year - 4').copy()  # filtra os dados para o ano mais recente e os 4 anos anteriores
    pevs_extracao['Município'] = pevs_extracao['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado

    # Pivot para ter variáveis como colunas
    pevs_extracao = pevs_extracao.pivot_table(
        index=['Município', 'Tipo de produto extrativo', 'Ano'], 
        columns='Variável', 
        values='Valor',
        aggfunc='sum'
    ).reset_index()
    
    # Merge com IPCA
    pevs_extracao = pevs_extracao.merge(df_ipca_annual, on='Ano', how='left')
    
    # Calcular valor real da produção
    pevs_extracao['Valor real da produção'] = pevs_extracao['Valor da produção na extração vegetal'] * pevs_extracao['Inflação']
    pevs_extracao = pevs_extracao.rename(columns={'Valor da produção na extração vegetal': 'Valor nominal da produção'})
    
    # Remover coluna de inflação
    pevs_extracao.drop(columns=['Inflação'], errors='ignore', inplace=True)

    # Calcular métricas e rankings por grupo
    pevs_extracao['Valor médio real unitário'] = np.where(
        (pevs_extracao['Quantidade produzida na extração vegetal'].notnull()) & 
        (pevs_extracao['Quantidade produzida na extração vegetal'] != 0),
        pevs_extracao['Valor real da produção'] / pevs_extracao['Quantidade produzida na extração vegetal'],
        np.nan
    )
        
    # Rankings por produto e ano
    for col in ['Quantidade produzida na extração vegetal', 'Valor nominal da produção', 'Valor real da produção', 'Valor médio real unitário']:
        pevs_extracao[f'Ranking {col}'] = pevs_extracao.groupby(['Tipo de produto extrativo', 'Ano'])[col].rank(method='min', ascending=False)
    
    # Limpar códigos dos produtos
    pevs_extracao['Tipo de produto extrativo'] = pevs_extracao['Tipo de produto extrativo'].str.replace(r'^\d+\.?\d* - ', '', regex=True)
    
    # Formatar data
    pevs_extracao['Ano'] = '31/12/' + pevs_extracao['Ano'].astype(str)

    pevs_extracao = pevs_extracao.loc[(pevs_extracao['Valor real da produção'].notnull()) & (pevs_extracao['Valor real da produção'] != 0)]  # remove linhas com valor nulo na produção
    
    # Salvar
    output_path = os.path.join(mart_path, 'producao_extracao_vegetal_silvicultra_extracao_consolidado.parquet')
    pevs_extracao.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    print(f'PEVS_extracao processado e salvo em: {output_path}')

# ========================
# PROCESSAMENTO SILVICULTURA PEV
# ========================

silvicultura_path = os.path.join(raw_path, 'producao_extracao_vegetal_silvicultra_silvicultura_pev', 'raw_producao_extracao_vegetal_silvicultra_silvicultura_pev_consolidado.parquet')

if os.path.exists(silvicultura_path):
    pevs_silvicultura = pd.read_parquet(silvicultura_path)[['Município', 'Variável', 'Tipo de produto da silvicultura', 'Ano', 'Valor']]
    
    max_year = pevs_silvicultura['Ano'].max()  # obtém o ano máximo presente na base
    pevs_silvicultura = pevs_silvicultura.query('Ano >= @max_year - 4').copy()  # filtra os dados para o ano mais recente e os 4 anos anteriores
    pevs_silvicultura['Município'] = pevs_silvicultura['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado

    # Pivot para ter variáveis como colunas
    pevs_silvicultura = pevs_silvicultura.pivot_table(
        index=['Município', 'Tipo de produto da silvicultura', 'Ano'], 
        columns='Variável', 
        values='Valor',
        aggfunc='sum'
    ).reset_index()
    
    # Merge com IPCA
    pevs_silvicultura = pevs_silvicultura.merge(df_ipca_annual, on='Ano', how='left')
    
    # Calcular valor real da produção
    pevs_silvicultura['Valor real da produção'] = pevs_silvicultura['Valor da produção na silvicultura'] * pevs_silvicultura['Inflação']
    pevs_silvicultura = pevs_silvicultura.rename(columns={'Valor da produção na silvicultura': 'Valor nominal da produção'})
    
    # Remover coluna de inflação
    pevs_silvicultura.drop(columns=['Inflação'], errors='ignore', inplace=True)
    
    # Calcular métricas e rankings por grupo
    pevs_silvicultura['Valor médio real unitário'] = np.where(
        (pevs_silvicultura['Quantidade produzida na silvicultura'].notnull()) & 
        (pevs_silvicultura['Quantidade produzida na silvicultura'] != 0),
        pevs_silvicultura['Valor real da produção'] / pevs_silvicultura['Quantidade produzida na silvicultura'],
        np.nan
    )
        
    # Rankings por produto e ano
    for col in ['Quantidade produzida na silvicultura', 'Valor nominal da produção', 'Valor real da produção', 'Valor médio real unitário']:
        pevs_silvicultura[f'Ranking {col}'] = pevs_silvicultura.groupby(['Tipo de produto da silvicultura', 'Ano'])[col].rank(method='min', ascending=False)
    
    # Limpar códigos dos produtos
    pevs_silvicultura['Tipo de produto da silvicultura'] = pevs_silvicultura['Tipo de produto da silvicultura'].str.replace(r'^\d+(\.\d+)* - ', '', regex=True)
    
    # Formatar data
    pevs_silvicultura['Ano'] = '31/12/' + pevs_silvicultura['Ano'].astype(str)

    pevs_silvicultura = pevs_silvicultura.loc[(pevs_silvicultura['Valor real da produção'].notnull()) & (pevs_silvicultura['Valor real da produção'] != 0)]  # remove linhas com valor nulo na produção
    
    # Salvar
    output_path = os.path.join(mart_path, 'producao_extracao_vegetal_silvicultra_silvicultura_consolidado.parquet')
    pevs_silvicultura.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    print(f'PEVS_silvicultura processado e salvo em: {output_path}')

print('Processamento mart PEV concluído!')