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
ipca_path = os.path.join(raw_path, 'ipca.parquet')
populacao_path = os.path.join(raw_path, 'raw_censo_demografico.parquet')
beneficiarios_path = os.path.join(raw_path, 'raw_beneficiarios_bolsa_familia.parquet')
pbf_path = os.path.join(raw_path, 'raw_programa_bolsa_familia.parquet')
qtd_path = os.path.join(raw_path, 'raw_programa_bolsa_familia_quantidade_beneficiarios.parquet')
valor_path = os.path.join(raw_path, 'raw_programa_bolsa_familia_valor_beneficios.parquet')
cobertura_path = os.path.join(raw_path, 'raw_programa_bolsa_familia_percentual_cobertura.parquet')
os.makedirs(mart_path, exist_ok=True)


try:

# ========================
# PROCESSAMENTO IPCA
# ========================

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


# ========================
# PROCESSAMENTO POPULAÇÃO
# ========================


    df_populacao = pd.read_parquet(populacao_path)[['Município', 'Valor']]
    df_populacao['Município'] = df_populacao['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    df_populacao.rename(columns={'Valor': 'População'}, inplace=True)
    

# ========================
# PROCESSAMENTO BENEFICIÁRIOS BOLSA FAMÍLIA
# ========================

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

    # join com ipca
    df_pbf_valores = df_pbf_valores.merge(df_ipca, on='Mês', how='left')
    df_pbf_valores['Inflação'] = df_pbf_valores['Inflação'].fillna(1)  # se não tiver inflação, considera 1 (sem correção)
    df_pbf_valores['Valor corrigido'] = df_pbf_valores['Valor'] * df_pbf_valores['Inflação']  # valor corrigido
    df_pbf_valores.drop(columns=['Inflação'], inplace=True)  # remove coluna desnecessária
    df_pbf_valores.sort_values(by=['Unidade Territorial', 'Mês', 'Variável'], inplace=True)  # ordena para cálculo da variação
    groupby_cols = ['Unidade Territorial', 'Variável']
    df_pbf_valores['Variação mensal nominal'] = (df_pbf_valores['Valor'] - df_pbf_valores.groupby(groupby_cols)['Valor'].shift(1)) / df_pbf_valores.groupby(groupby_cols)['Valor'].shift(1) * 100
    df_pbf_valores['Variação mensal real'] = (df_pbf_valores['Valor corrigido'] - df_pbf_valores.groupby(groupby_cols)['Valor corrigido'].shift(1)) / df_pbf_valores.groupby(groupby_cols)['Valor corrigido'].shift(1) * 100
    # substituir NaN ou infinito em colunas numéricas por np.nan
    num_cols = df_pbf_valores.select_dtypes(include=[np.number]).columns
    df_pbf_valores[num_cols] = df_pbf_valores[num_cols].where(np.isfinite(df_pbf_valores[num_cols]), np.nan)

    # terceira parte do processamento
    keep_cols = [col for col in df_pbf.columns if not col.startswith('Valor')]
    df_pbf_quantidades = df_pbf[keep_cols].copy()
    df_pbf_quantidades = df_pbf_quantidades.melt(
        id_vars=['UF', 'Unidade Territorial', 'Mês'],
        value_vars=[col for col in df_pbf_quantidades.columns if col not in ['UF', 'Unidade Territorial', 'Mês']],
        var_name='Variável',
        value_name='Valor'
    )

    df_pbf_final = pd.concat([df_pbf_valores, df_pbf_quantidades], ignore_index=True)  # concatena os dois dataframes


# ========================
# PROCESSAMENTO PROGRAMA BOLSA FAMÍLIA - VALOR DOS BENEFÍCIOS
# ========================

    df_valor = pd.read_parquet(valor_path).query('UF == "SE"')
    df_valor.rename(columns={'Referência': 'Mês'}, inplace=True)
    df_valor['Mês'] = pd.to_datetime(df_valor['Mês'], format='%m/%Y')  # converte para datetime
    df_valor.drop(columns=['Código'], inplace=True)  # remove coluna desnecessária
    df_valor = df_valor.melt(
        id_vars=['UF', 'Unidade Territorial', 'Mês'],
        value_vars=[col for col in df_valor.columns if col not in ['UF', 'Unidade Territorial', 'Mês']],
        var_name='Variável',
        value_name='Valor'
    )

    # join com ipca
    df_valor = df_valor.merge(df_ipca, on='Mês', how='left')
    df_valor['Inflação'] = df_valor['Inflação'].fillna(1)  # se não tiver inflação, considera 1 (sem correção)
    df_valor['Valor corrigido'] = df_valor['Valor'] * df_valor['Inflação']  # valor corrigido
    df_valor.drop(columns=['Inflação'], inplace=True)  # remove coluna desnecessária
    df_valor.sort_values(by=['Unidade Territorial', 'Mês', 'Variável'], inplace=True)  # ordena para cálculo da variação
    groupby_cols = ['Unidade Territorial', 'Variável']
    df_valor['Variação mensal nominal'] = (df_valor['Valor'] - df_valor.groupby(groupby_cols)['Valor'].shift(1)) / df_valor.groupby(groupby_cols)['Valor'].shift(1) * 100
    df_valor['Variação mensal real'] = (df_valor['Valor corrigido'] - df_valor.groupby(groupby_cols)['Valor corrigido'].shift(1)) / df_valor.groupby(groupby_cols)['Valor corrigido'].shift(1) * 100
    # substituir NaN ou infinito em colunas numéricas por np.nan
    num_cols = df_valor.select_dtypes(include=[np.number]).columns
    df_valor[num_cols] = df_valor[num_cols].where(np.isfinite(df_valor[num_cols]), np.nan)


# ========================
# PROCESSAMENTO PROGRAMA BOLSA FAMÍLIA - PERCENTUAL DE COBERTURA
# ========================

    df_cobertura = pd.read_parquet(cobertura_path).query('UF == "SE"')
    df_cobertura.rename(columns={'Referência': 'Mês'}, inplace=True)
    df_cobertura['Mês'] = pd.to_datetime(df_cobertura['Mês'], format='%m/%Y')  # converte para datetime
    df_cobertura.drop(columns=['Código'], inplace=True)  # remove coluna desnecessária
    df_cobertura = df_cobertura.melt(
        id_vars=['UF', 'Unidade Territorial', 'Mês'],
        value_vars=[col for col in df_cobertura.columns if col not in ['UF', 'Unidade Territorial', 'Mês']],
        var_name='Variável',
        value_name='Valor'
    )


# ========================
# JUNÇÃO DE TODOS OS DATAFRAMES
# ========================

    df_bolsa_familia = pd.concat([
        df_beneficiarios,
        df_pbf_final,
        df_qtd,
        df_valor,
        df_cobertura
    ], ignore_index=True)
    df_bolsa_familia.drop(columns=['UF'], inplace=True)  # remove coluna desnecessária
    df_bolsa_familia.sort_values(by=['Unidade Territorial', 'Mês', 'Variável'], inplace=True)  # ordena para cálculo da variação
    groupby_cols = ['Mês', 'Variável']
    df_bolsa_familia['Ranking Valor'] = df_bolsa_familia.groupby(groupby_cols)['Valor'].rank(ascending=False, method='min')
    df_bolsa_familia['Ranking Valor corrigido'] = df_bolsa_familia.groupby(groupby_cols)['Valor corrigido'].rank(ascending=False, method='min')
    df_bolsa_familia['Ranking Variação mensal nominal'] = df_bolsa_familia.groupby(groupby_cols)['Variação mensal nominal'].rank(ascending=False, method='min')
    df_bolsa_familia['Ranking Variação mensal real'] = df_bolsa_familia.groupby(groupby_cols)['Variação mensal real'].rank(ascending=False, method='min')
    df_bolsa_familia = df_bolsa_familia.loc[df_bolsa_familia['Mês'].dt.year >= 2019]  # remove a linha do estado inteiro
    df_bolsa_familia['Mês'] = df_bolsa_familia['Mês'].dt.strftime('%d/%m/%Y')  # formata para DD/MM/YYYY

    df_bolsa_familia.to_parquet(os.path.join(mart_path, 'bolsa_familia.parquet'), index=False)

except:
    print(traceback.format_exc())