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
populacao_path = os.path.join(raw_path, 'raw_censo_demografico.parquet')
beneficiarios_path = os.path.join(raw_path, 'raw_beneficiarios_bolsa_familia.parquet')
pbf_path = os.path.join(raw_path, 'raw_programa_bolsa_familia.parquet')
qtd_path = os.path.join(raw_path, 'raw_programa_bolsa_familia_quantidade_beneficiarios.parquet')
valor_path = os.path.join(raw_path, 'raw_programa_bolsa_familia_valor_beneficios.parquet')
cobertura_path = os.path.join(raw_path, 'raw_programa_bolsa_familia_percentual_cobertura.parquet')
os.makedirs(mart_path, exist_ok=True)


# ========================
# PROCESSAMENTO IPCA
# ========================

if os.path.exists(ipca_path):
    df_ipca = pd.read_parquet(ipca_path)
    df_ipca.rename(columns={'Mes': 'Mês'}, inplace=True)  # renomeia para Ano, para facilitar o merge

    ano_mais_recente = df_ipca.loc[df_ipca['indice_ipca'].notnull(), 'Mês'].max()  # ano mais recente com índice disponível
    max_ipca = df_ipca.loc[df_ipca['Mês'] == ano_mais_recente, 'indice_ipca'].max()  # índice do ano mais recente
    df_ipca['Inflação'] = np.where(
        (df_ipca['indice_ipca'].notnull()) & (df_ipca['indice_ipca'] != 0),
        max_ipca / df_ipca['indice_ipca'],
        np.nan
    )  # fator de correção para valores reais
    df_ipca.drop(columns=['indice_ipca'], inplace=True)  # remove coluna desnecessária

    print(f'IPCA: {len(df_ipca)} registros')
else:
    print('ERRO: Base IPCA não encontrada')


# ========================
# PROCESSAMENTO POPULAÇÃO
# ========================

if os.path.exists(populacao_path):
    df_populacao = pd.read_parquet(populacao_path)[['Município', 'Valor']]
    df_populacao['Município'] = df_populacao['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    df_populacao.rename(columns={'Valor': 'População'}, inplace=True)
    

# ========================
# PROCESSAMENTO BENEFICIÁRIOS BOLSA FAMÍLIA
# ========================

if os.path.exists(beneficiarios_path):
    df_beneficiarios = pd.read_parquet(beneficiarios_path)[[
        'UF', 'Unidade Territorial', 'Referência', 'Beneficiários até Out/21', 'Beneficiários a partir Mar/23'
    ]].query('UF == "SE"')
    df_beneficiarios.rename(columns={'Referência': 'Mês'}, inplace=True)
    df_beneficiarios['Mês'] = pd.to_datetime(df_beneficiarios['Mês'], format='%m/%Y')  # converte para datetime
    df_beneficiarios = df_beneficiarios.melt(
        id_vars=['UF', 'Unidade Territorial', 'Mês'],
        value_vars=['Beneficiários até Out/21', 'Beneficiários a partir Mar/23'],
        var_name='Variável',
        value_name='Valor'
    )


# ========================
# PROCESSAMENTO PROGRAMA BOLSA FAMÍLIA - QUANTIDADE DE BENEFICIÁRIOS
# ========================

if os.path.exists(qtd_path):
    df_qtd = pd.read_parquet(qtd_path).query('UF == "SE"')
    df_qtd.rename(columns={'Mês/Ano': 'Mês'}, inplace=True)
    df_qtd['Mês'] = pd.to_datetime(df_qtd['Mês'], format='%m/%Y')  # converte para datetime
    df_qtd.drop(columns=['Código'], inplace=True)  # remove coluna desnecessária
    df_qtd = df_qtd.melt(
        id_vars=['UF', 'Unidade Territorial', 'Mês'],
        value_vars=[col for col in df_qtd.columns if col not in ['UF', 'Unidade Territorial', 'Mês']],
        var_name='Variável',
        value_name='Valor'
    )


# ========================
# PROCESSAMENTO PROGRAMA BOLSA FAMÍLIA
# ========================

if os.path.exists(pbf_path):
    df_pbf = pd.read_parquet(pbf_path).query('UF == "SE"')
    df_pbf.rename(columns={'Referência': 'Mês'}, inplace=True)
    df_pbf['Mês'] = pd.to_datetime(df_pbf['Mês'], format='%m/%Y')  # converte para datetime
    df_pbf.drop(columns=['Código'], inplace=True)  # remove coluna desnecessária

    # segunda parte do processamento
    val_cols = [col for col in df_pbf.columns if not col.startswith('Famílias') and col not in ['UF', 'Unidade Territorial', 'Mês']]
    df_pbf_valores = df_pbf.melt(
        id_vars=['UF', 'Unidade Territorial', 'Mês'],
        value_vars=val_cols,
        var_name='Variável',
        value_name='Valor'
    )


"""
CONTINUAR A PARTIR DE

ipca_valores <- ipca |> 
  filter(Mês %in% pbf_valores$Mês) |> 
  mutate(inflacao = tail(indice_ipca, n = 1)/indice_ipca) |> 
  select(-indice_ipca)
"""



print('OK')