# image Python comme base
FROM python:3.10-slim

# Installer des dépendances système si nécessaire
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le code dans le conteneur
COPY . .

# Commande pour démarrer le script Python
CMD ["python", "etl_data_streaming.py"]