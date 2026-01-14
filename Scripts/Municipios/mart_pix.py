import traceback
import functions as c
import os
import pandas as pd
import numpy as np


# caminhos
raw_path = c.raw_path
mart_path = c.mart_path
error_path = c.error_path
pix_path = os.path.join(raw_path, 'pix', 'raw_pix_consolidado.parquet')
municipios_path = os.path.join(raw_path, 'municipios.csv')
ipca_path = os.path.join(raw_path, 'ipca.parquet')
os.makedirs(mart_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

try:
    df_pix = pd.read_parquet(pix_path)
    df_mun = pd.read_csv(municipios_path, sep=';', dtype={'cod_ibge': str})

    df_sergipe = df_pix.copy()
    df_sergipe['Mês'] = pd.to_datetime(df_sergipe['AnoMes'].astype(str), format='%Y%m')
    df_sergipe.drop(columns=['AnoMes', 'Estado'], inplace=True)
    df_sergipe.drop_duplicates(inplace=True)
    df_sergipe.rename(columns={'Municipio_Ibge': 'cod_ibge'}, inplace=True)
    df_sergipe['cod_ibge'] = df_sergipe['cod_ibge'].astype(str)
    df_sergipe['VL pago total'] = df_sergipe['VL_PagadorPF'] + df_sergipe['VL_PagadorPJ']
    df_sergipe['QT de transações pagas'] = df_sergipe['QT_PagadorPF'] + df_sergipe['QT_PagadorPJ']
    df_sergipe['VL recebido total'] = df_sergipe['VL_RecebedorPF'] + df_sergipe['VL_RecebedorPJ']
    df_sergipe['QT de transações recebidas'] = df_sergipe['QT_RecebedorPF'] + df_sergipe['QT_RecebedorPJ']
    df_sergipe['VL médio pago total'] = df_sergipe['VL pago total'] / df_sergipe['QT de transações pagas']
    df_sergipe['VL médio pago PF'] = df_sergipe['VL_PagadorPF'] / df_sergipe['QT_PagadorPF']
    df_sergipe['VL médio pago PJ'] = df_sergipe['VL_PagadorPJ'] / df_sergipe['QT_PagadorPJ']
    df_sergipe['VL médio recebido total'] = df_sergipe['VL recebido total'] / df_sergipe['QT de transações recebidas']
    df_sergipe['VL médio recebido PF'] = df_sergipe['VL_RecebedorPF'] / df_sergipe['QT_RecebedorPF']
    df_sergipe['VL médio recebido PJ'] = df_sergipe['VL_RecebedorPJ'] / df_sergipe['QT_RecebedorPJ']

    df_sergipe = df_sergipe.merge(df_mun, on='cod_ibge', how='left')
    df_sergipe.drop(columns=['cod_ibge'], inplace=True)

    cols = [col for col in df_sergipe.columns if not col.startswith('QT')]
    df_valores = df_sergipe[cols].copy()
    df_valores = df_valores.melt(id_vars=['Município', 'Mês'], var_name="Variável", value_name="Valor")

    # TRATAMENTO DE INFLAÇÃO USANDO IPCA
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

    df_valores = df_valores.merge(df_ipca, on='Mês', how='left')
    df_valores.sort_values(by=['Município', 'Mês', 'Variável'], inplace=True)
    df_valores['Inflação'] = df_valores['Inflação'].fillna(1)
    df_valores['Valor corrigido'] = df_valores['Valor'] * df_valores['Inflação']
    df_valores.drop(columns=['Inflação'], inplace=True)
    df_valores['Variação mensal nominal'] = df_valores.groupby(['Município', 'Variável'])['Valor'].pct_change() * 100
    df_valores['Variação mensal real'] = df_valores.groupby(['Município', 'Variável'])['Valor corrigido'].pct_change() * 100
    df_valores['Valor nominal acumulado em 12 meses'] = df_valores.groupby(['Município', 'Variável'])['Valor'].transform(lambda x: x.rolling(window=12).sum())
    df_valores['Valor real acumulado em 12 meses'] = df_valores.groupby(['Município', 'Variável'])['Valor corrigido'].transform(lambda x: x.rolling(window=12).sum())
    df_valores.replace([np.inf, -np.inf], np.nan, inplace=True)

    df_quantidades = df_sergipe.copy()
    cols = [col for col in df_sergipe.columns if not col.startswith('VL')]
    df_quantidades = df_quantidades[cols]
    df_quantidades = df_quantidades.melt(id_vars=['Município', 'Mês'], var_name="Variável", value_name="Valor")

    df_final = pd.concat([df_valores, df_quantidades], ignore_index=True)
    df_final['Ranking valor'] = df_final.groupby(['Mês', 'Variável'])['Valor'].rank(ascending=False, method='min')
    df_final['Ranking valor corrigido'] = df_final.groupby(['Mês', 'Variável'])['Valor corrigido'].rank(ascending=False, method='min')
    df_final['Ranking variação mensal nominal'] = df_final.groupby(['Mês', 'Variável'])['Variação mensal nominal'].rank(ascending=False, method='min')
    df_final['Ranking variação mensal real'] = df_final.groupby(['Mês', 'Variável'])['Variação mensal real'].rank(ascending=False, method='min')
    df_final['Ranking valor nominal acumulado em 12 meses'] = df_final.groupby(['Mês', 'Variável'])['Valor nominal acumulado em 12 meses'].rank(ascending=False, method='min')
    df_final['Ranking valor real acumulado em 12 meses'] = df_final.groupby(['Mês', 'Variável'])['Valor real acumulado em 12 meses'].rank(ascending=False, method='min')
    # Adaptação da ordem das colunas (relocate(Mês, .after = Variável))
    cols = ['Município', 'Variável', 'Mês'] + [col for col in df_final.columns if col not in ['Município', 'Variável', 'Mês']]
    df_final = df_final[cols]
    df_final['Variável'] = df_final['Variável'].replace({
        "QT de transações pagas": "Quantidade de transações pagas",
        "QT de transações recebidas": "Quantidade de transações recebidas",
        "QT_PagadorPF": "Quantidade de transações pagas por pessoa física",
        "QT_PagadorPJ": "Quantidade de transações pagas por pessoa jurídica",
        "QT_RecebedorPF": "Quantidade de transações recebidas por pessoa física",
        "QT_RecebedorPJ": "Quantidade de transações recebidas por pessoa jurídica",
        "VL médio pago PF": "Valor médio pago por pessoa física",
        "VL médio pago PJ": "Valor médio pago por pessoa jurídica",
        "VL médio pago total": "Valor médio pago total",
        "VL médio recebido PF": "Valor médio recebido por pessoa física",
        "VL médio recebido PJ": "Valor médio recebido por pessoa jurídica",
        "VL médio recebido total": "Valor médio recebido total",
        "VL pago total": "Valores pagos total",
        "VL recebido total": "Valores recebidos total",
        "VL_PagadorPF": "Valores pagos por pessoa física",
        "VL_PagadorPJ": "Valores pagos por pessoa jurídica",
        "VL_RecebedorPF": "Valores recebidos por pessoa física",
        "VL_RecebedorPJ": "Valores recebidos por pessoa jurídica"
    })

    ordem_variavel = [
        "Valores pagos total",
        "Valores pagos por pessoa física", 
        "Valores pagos por pessoa jurídica",
        "Valores recebidos total",
        "Valores recebidos por pessoa física",
        "Valores recebidos por pessoa jurídica",
        "Quantidade de transações pagas",
        "Quantidade de transações pagas por pessoa física",
        "Quantidade de transações pagas por pessoa jurídica", 
        "Quantidade de transações recebidas",
        "Quantidade de transações recebidas por pessoa física",
        "Quantidade de transações recebidas por pessoa jurídica",
        "Valor médio pago total",
        "Valor médio pago por pessoa física",
        "Valor médio pago por pessoa jurídica",
        "Valor médio recebido total",
        "Valor médio recebido por pessoa física",
        "Valor médio recebido por pessoa jurídica"
    ]

    
    df_final['Variável'] = pd.Categorical(df_final['Variável'], categories=ordem_variavel, ordered=True)
    df_final = df_final.sort_values(by=['Município', 'Mês', 'Variável'])
    
    df_final['Mês'] = df_final['Mês'].dt.strftime('%d/%m/%Y')
    df_final = df_final.assign(
        Tipo=np.where(df_final['Variável'].str.contains('Pagador|pago|pagas'), 'Pago',
            np.where(df_final['Variável'].str.contains('Recebedor|recebido|recebidas'), 'Recebido', 'desconhecido')),
        Agente=np.where(df_final['Variável'].str.contains('PF|física'), 'PF',
            np.where(df_final['Variável'].str.contains('PJ|jurídica'), 'PJ',
            np.where(df_final['Variável'].str.contains('total'), 'Total', 'Total'))),
        Vl_ou_Qtd=np.where(df_final['Variável'].str.contains('Valores'), 'Valor',
            np.where(df_final['Variável'].str.contains('médio'), 'Valor médio',
            np.where(df_final['Variável'].str.contains('Quantidade'), 'Quantidade', 'desconhecido')))
    )

    # salva o arquivo consolidado e tratado
    pix_path = os.path.join(mart_path, 'pix.parquet')
    df_final.to_parquet(pix_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Consolidado salvo: {len(df_final)} registros totais')
    print(f'Arquivo: {pix_path}')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_mart_pix.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em mart_pix.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)