import traceback
import functions as c
import os
import pandas as pd
from time import sleep


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
db_name = 'programa_bolsa_familia_percentual_cobertura'  # nome base da base de dados
file_name = 'raw_' + db_name + '.parquet'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    print('Iniciando download dos dados de percentual de cobertura do programa bolsa família...')

    # URL e headers para a API
    url = "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=r6JtZI%2B0gbBtxKW25rV%2FfmhkhJFkl21kmK19ZnB1ZXGmaX7KlmONl2ybcZLC7sGdqampo%2B%2BarLSYmsDNnMuwks3qr6ahu5Sc35iws6WgxpNjm21uw9y5p6GDqazunoiJnY7D1JileKbS6HCvXauWrd5ZxLacm3ehoM%2Bwkr7pvHB5b2dqq2p6f2dah5JairGbwultdJ6ulKvtnay0mJp3xp%2FdolPL8LmgXK2jnpq2iLSYmcrGbtCen9DgiG%2BiqaGt3nSIwayaetxUzZ6mwpvEnKG2VXrmnsCtmJvGnXCRb2OPrHplbHVla6BZwbacm3eJc8yjks7vsZOiqaJpuZiDhG1miJWSz7CnxuiuqKW%2Blpnfmrq3o5a41JLarJXP4MCTnq6Und6nwL1pXYiRbaSrqMrgv52fcV9rqWltbpyZysZT2LKfyZuyoqBpsnXfmrnBnGi9wp%2Fdom6Y4a6gr61wdeyuunGyTrrCps9dqsXgu1R8tZqt2Jq7vXVqfpNjnHBgja56ZG1vVa7hnrtud4%2B9wKTeoZLD3LpUobSon5mnwrqjTbzPl4u6bsPcuaehg5ub5ayyiXKTuM2mz3hu0PC6V7dpmJvsnm3Fn5LFgXPXoqa83LujeoVcbKlrgHtnYISRZJFdp8Xgu1RkiJeg2KrBspaTuM5iqpxpk7GGZXCnmq3torqvq5bNwpLQnqDG57aVr6elqdurssGWj73Als%2BrpsytfWVsgm%2Bo7qaywKCQgItkmm1TwufAmVy2qqblWbK8m07UnJnLqabCtrOVqLuadbSfrrqqkpKcpt%2Bqr6LuwZ2pqamj75ptspxNncKgLeqfxtzAVIy3l6zerG17V3C8z6bZXXyfwpJUbnhmapyKwq%2BlocDFlM6iU8HgbXqdtfjn5aKuwVePvM%2BY0KaWxtyxla9opZ%2FlqG2QppnKwlOwnqAgKLmdnWhdm%2B389m6GosvWldysYo%2Brf2Vla4Wf65yyvKuiuM1TzqJTwOqvma68qqzaWbGvqk2dwqAt6p%2FG3MBUnq2jn9%2BisLf6zsnKlN1dl8ybnXaCaF177fz2boaiy9aV3Kxij6x2V429lqjtorGvm5J3xZiKg5TKPvqgpamoWtueu7OdlrrKlM6epn3rsqCraHep5ayubn2OxCTg1qaUfaOupP%2F7qFrGmr8R3pyGk2OccFyAy7Kmn62jru6auW6bknfEosyipdHwv5VcrJatmX%2Buu%2Fraw8qU3V2VwumymqWrnv0aq7avqk270FO6f3l9o66k%2F%2FuoWsaavxHenIaTY5xwXNnriLC4xGdqqnB6fmhah5KHmm1tjauHZGyicA%3D%3D&ag=m&wt=json&tp_funcao_consulta=0&rqprocess=1c34a09443b2c68b3f7b9de7ae5288e0"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "DNT": "1",
        "Origin": "https://aplicacoes.cidadania.gov.br",
        "Referer": "https://aplicacoes.cidadania.gov.br/vis/data3/v.php",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }

    # Coleta todos os dados
    all_data = []
    start = 0
    length = 25000  # Tamanho conservador para evitar timeout
    draw = 1
    max_retries = 3

    # Cria sessão robusta com retry
    session = c.create_session_with_retries()

    print('Fazendo primeira requisição para descobrir total de registros...')

    # Payload base para as requisições (baseado no curl fornecido)
    def create_payload(draw, start, length):
        return {
            "draw": str(draw),
            "columns[0][data]": "0",
            "columns[0][name]": "codigo",
            "columns[0][searchable]": "true",
            "columns[0][orderable]": "true",
            "columns[0][search][value]": "",
            "columns[0][search][regex]": "false",
            "columns[1][data]": "1",
            "columns[1][name]": "nome",
            "columns[1][searchable]": "true",
            "columns[1][orderable]": "true",
            "columns[1][search][value]": "",
            "columns[1][search][regex]": "false",
            "columns[2][data]": "2",
            "columns[2][name]": "sigla",
            "columns[2][searchable]": "true",
            "columns[2][orderable]": "true",
            "columns[2][search][value]": "",
            "columns[2][search][regex]": "false",
            "columns[3][data]": "3",
            "columns[3][name]": "mes_ano_formatado",
            "columns[3][searchable]": "true",
            "columns[3][orderable]": "true",
            "columns[3][search][value]": "",
            "columns[3][search][regex]": "false",
            "columns[4][data]": "4",
            "columns[4][name]": "_666914_estimativa_familias_pobres_bf_censo2010",
            "columns[4][searchable]": "true",
            "columns[4][orderable]": "true",
            "columns[4][search][value]": "",
            "columns[4][search][regex]": "false",
            "columns[5][data]": "5",
            "columns[5][name]": "(case when t.mes_ano<='2021-10-01' then t.bf_qtd_fam else null",
            "columns[5][searchable]": "true",
            "columns[5][orderable]": "true",
            "columns[5][search][value]": "",
            "columns[5][search][regex]": "false",
            "columns[6][data]": "6",
            "columns[6][name]": "(case when t.mes_ano<='2021-10-01' then (t.bf_qtd_fam/t._666914",
            "columns[6][searchable]": "true",
            "columns[6][orderable]": "true",
            "columns[6][search][value]": "",
            "columns[6][search][regex]": "false",
            "columns[7][data]": "7",
            "columns[7][name]": "(case when t.mes_ano>='2023-03-01' then t.bf_qtd_fam else null",
            "columns[7][searchable]": "true",
            "columns[7][orderable]": "true",
            "columns[7][search][value]": "",
            "columns[7][search][regex]": "false",
            "columns[8][data]": "8",
            "columns[8][name]": "(case when t.mes_ano>='2023-03-01' then (t.bf_qtd_fam/t._666914",
            "columns[8][searchable]": "true",
            "columns[8][orderable]": "true",
            "columns[8][search][value]": "",
            "columns[8][search][regex]": "false",
            "order[0][column]": "2",
            "order[0][dir]": "asc",
            "order[1][column]": "0",
            "order[1][dir]": "asc",
            "start": str(start),
            "length": str(length),
            "search[value]": "",
            "search[regex]": "false"
        }

    payload = create_payload(draw, start, length)

    # Primeira requisição com retry
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = session.post(url, headers=headers, data=payload, timeout=30)
            response.raise_for_status()
            first_page = response.json()
            break
        except Exception as e:
            retry_count += 1
            print(f"Erro na primeira requisição (tentativa {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                raise
            print("Aguardando 5 segundos antes de tentar novamente...")
            sleep(5)

    records_total = first_page["recordsTotal"]
    print(f"Total de registros encontrados: {records_total}")

    all_data.extend(first_page["data"])

    # Continua coletando até obter todos os registros
    while len(all_data) < records_total:
        start += length
        draw += 1
        print(f"Coletando página com start={start}... ({len(all_data)}/{records_total} coletados)")
        
        payload = create_payload(draw, start, length)
        
        # Requisição com retry
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = session.post(url, headers=headers, data=payload, timeout=30)
                response.raise_for_status()
                page = response.json()
                break
            except Exception as e:
                retry_count += 1
                print(f"Erro na requisição (tentativa {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    print(f"Falha após {max_retries} tentativas. Dados coletados até agora: {len(all_data)}")
                    raise
                print("Aguardando 5 segundos antes de tentar novamente...")
                sleep(5)
        
        all_data.extend(page["data"])
        
        # Pausa entre requisições para não sobrecarregar o servidor
        sleep(1)

    print(f"Total coletado: {len(all_data)} registros")

    # ************************
    # PROCESSAMENTO DOS DADOS
    # ************************

    print('Processando e limpando os dados...')

    # Converte lista de listas em DataFrame
    data = pd.DataFrame(all_data)

    # Define os nomes das colunas conforme especificado
    column_names = [
        'Código',
        'Unidade Territorial',
        'UF',
        'Referência',
        'Estimativa de Famílias Pobres - Censo IBGE 2010',
        'Quantidade de Famílias beneficiadas pelo Bolsa Família (até Outubro/2021)',
        'Percentual de cobertura das Famílias beneficiárias do PBF (Até Outubro/21)',
        'Quantidade de Famílias beneficiadas pelo Bolsa Família (após Março/2023)',
        'Percentual de cobertura das Famílias beneficiárias do PBF (após Março/2023)'
    ]
    data.columns = column_names

    # Converte tipos de dados
    data['Código'] = data['Código'].astype(str)
    data['Unidade Territorial'] = data['Unidade Territorial'].astype('category')
    data['UF'] = data['UF'].astype('category')
    data['Referência'] = data['Referência'].astype(str)

    # Colunas numéricas - quantidade de famílias
    quantity_columns = [
        'Estimativa de Famílias Pobres - Censo IBGE 2010',
        'Quantidade de Famílias beneficiadas pelo Bolsa Família (até Outubro/2021)',
        'Quantidade de Famílias beneficiadas pelo Bolsa Família (após Março/2023)'
    ]

    for col in quantity_columns:
        data[col] = pd.to_numeric(
            data[col].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('%', '').str.strip(),
            errors='coerce'
        )

    # Colunas de percentual - tratamento especial para percentuais
    percentage_columns = [
        'Percentual de cobertura das Famílias beneficiárias do PBF (Até Outubro/21)',
        'Percentual de cobertura das Famílias beneficiárias do PBF (após Março/2023)'
    ]

    for col in percentage_columns:
        data[col] = pd.to_numeric(
            data[col].astype(str).str.replace('.', '').str.replace(',', '.').str.replace('-', '').str.replace('%', '').str.strip(), 
            errors='coerce'
        )

    print(f'Dados processados: {data.shape[0]} linhas, {data.shape[1]} colunas')

    # Salva o arquivo
    print(f'Salvando dados em: {file_path}')
    data.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Arquivo {file_name} salvo com sucesso!')

    print('Download e processamento concluído!')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_programa_bolsa_familia_percentual_cobertura.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_programa_bolsa_familia_percentual_cobertura.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_programa_bolsa_familia_percentual_cobertura.txt".')