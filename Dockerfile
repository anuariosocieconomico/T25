# Usar a imagem oficial do Python
FROM python:3.10-slim

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o conteúdo do repositório para dentro do contêiner
COPY . /app

# Instalar as dependências do projeto
RUN pip install --no-cache-dir -r Scripts/Daniel/requirements.txt

# Comando para executar o script
CMD ["python", "Scripts/Daniel/run_scripts.py"]
