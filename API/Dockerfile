# Utilisation de l'image Python comme base
FROM python:3.9

WORKDIR /app

# Copie du fichier requirements.txt dans le conteneur
COPY requirements.txt .

# Installation les dépendances Python
RUN pip install -r requirements.txt

# Copie du script de l'API
COPY ./models/ ./models/
COPY opa_api.py .

# Exposition du port sur lequel l'application va tourner
EXPOSE 8000

# Commande pour démarrer le serveur
CMD ["uvicorn", "opa_api:api", "--host", "0.0.0.0", "--port", "8000"]