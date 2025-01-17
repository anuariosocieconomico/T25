# Use uma imagem base com Python
FROM python:3.10.11-slim

# Configurações do ambiente de trabalho
WORKDIR /app

# Copie os requisitos do projeto e instale as dependências
COPY Scripts/Daniel/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código para o contêiner
COPY . .

# Comando padrão para rodar scripts (pode ser ajustado)
CMD ["python", "main.py"]
