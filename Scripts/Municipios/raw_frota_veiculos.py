import datetime
import traceback
import functions as c
import os
import pandas as pd
import requests
from lxml import html as lxml_html
import io


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
db_name = 'frota_veiculos'  # nome base da base de dados
file_name = f'raw_{db_name}.parquet'  # nome do arquivo parquet
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    session = c.create_session_with_retries()
    url = 'https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/estatisticas-frota-de-veiculos-senatran'
    html = session.get(url, timeout=session.request_timeout, headers=c.headers).text
    
    """
    div[@property="rnews:articleBody"] contém os elementos <p> com os links de cada ano
    o primeiro looping extrai os links e anos, gerando o DataFrame df_links que será usado para baixar os dados no segundo looping
    """

    # Parse do HTML
    tree = lxml_html.fromstring(html)
    
    # Encontra a div com property="rnews:articleBody"
    article_div = tree.xpath('//div[@property="rnews:articleBody"]')[0]
    
    # Lista para armazenar os dados extraídos
    links_data = []
    
    print("Navegando pelo HTML e extraindo links...")
    print("=" * 80)
    
    # Itera por cada <p> dentro da div
    for i, p_element in enumerate(article_div.xpath('.//p'), 1):
        p_links = []
        
        # Para cada <a> dentro do <p>
        for a_element in p_element.xpath('.//a'):
            href = a_element.get('href')
            text = a_element.text_content().strip()
            
            # Só adiciona se tiver href e texto
            if href and text:
                link_info = {
                    'link': href,
                    'ano': text
                }
                links_data.append(link_info)
                p_links.append(f"{text} -> {href}")
    
    # Converte para DataFrame
    df_links = pd.DataFrame(links_data)
    df_links['ano'] = pd.to_numeric(df_links['ano'], errors='coerce')
    df_links = df_links.query('ano >= 2019').reset_index(drop=True)
    df_links['file_link'] = None
    df_links.sort_values(by='ano', ascending=False, inplace=True)
    
    """
    Segundo looping para extrair o link específico de "Frota por Município" de cada página de ano
    div[@id="parent-fieldname-text"] contém os links específicos de "Frota por Município"
    após encontrar o primeiro elemento <a> com esse texto, extrai o href e armazena na coluna file_link do DataFrame df_links
    o link será de dezembro ou do mês mais recente disponível
    o break evita que continue procurando outros links na mesma página após encontrar o primeiro
    """

    for index, row in df_links.iterrows():
        link = row['link']
        year = row['ano']
        print(f"\nProcessando ano {year}: {link}")
        
        r = session.get(link, timeout=session.request_timeout, headers=c.headers)
        r.raise_for_status()  # levanta erro se a requisição falhar
        
        # Parse do HTML da página específica do ano
        page_tree = lxml_html.fromstring(r.text)
        
        # Encontra a div com id="parent-fieldname-text"
        div = page_tree.xpath('//div[@id="parent-fieldname-text"]')[0]

        for a_element in div.xpath('.//a'):
            if 'frota por município' in a_element.text_content().lower().strip():
                href = a_element.get('href')
                df_links.loc[index, 'file_link'] = href
                print(f"Link do ano {year}: {href}")
                break

    """
    Terceiro looping para baixar os dados de cada link de cada ano
    """

    files = []
    for index, row in df_links.iterrows():
        file_link = row['file_link']
        year = row['ano']
        if pd.notna(file_link):
            print(f"Baixando dados do ano {year}...")
            r = session.get(file_link, timeout=session.request_timeout, headers=c.headers)
            r.raise_for_status()  # levanta erro se a requisição falhar

            df = pd.read_excel(io.BytesIO(r.content), skiprows=3)

            if 'UF' not in df.columns or 'MUNICIPIO' not in df.columns:
                df = pd.read_excel(io.BytesIO(r.content), skiprows=2)

            cols = [col for col in df.columns if col not in ['UF', 'MUNICIPIO']]
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            df['ANO'] = year
            df = df.query('UF == "SE"')

            files.append(df)

    df = pd.concat(files, ignore_index=True)
    df.to_parquet(file_path, index=False)
    print(f"\nDados salvos em {file_path}")

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_frota_veiculos.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_frota_veiculos.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_frota_veiculos.txt".')

