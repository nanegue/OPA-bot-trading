# OPA-bot-trading
Creation d un bot de trading basé sur un modèle de Machine Learning 

**** Récolte des données (extraction et transformation
Le but sera de créer une fonction de récupération de données générique afin de pouvoir avoir les données de n’importe quel marché.
Il faudra aussi créer un script de pré-processing pour réorganiser les données sortant du streaming afin qu’elles soient propres.
Récupérer les données historiques, pré-processer pour pouvoir entraîner notre futur modèle.
les données sont issus de la plateforme Binance en utilisant des fonctions fourni par l'API binance compatibles avec le langage python 

*** Stockage de la donnée
les données historiques ou de streaming extraites et transformés lors de l'étape précedente vont être stockées dans un base de données posgresql hebergés dans une instance  EC2 d' AWS (Amazon Web Services)

*** Implemenetation et Entrainement de plusieurs modèles de machine learning via AIrflow
utiliser les données historiques stockées en base de données pour créer des échantillons d'entrainement et de test pour differents modeles de ML. pour en choisir un au final afin de l'integrer dans l'API de prediction par la suite.

*** API de et de prediction 
créer une API qui permet de :
     - fournir les informations de bougie pour une crypto à une donnée donnée
     - fournir une prediction et un conseil d'investissement (achat ou vente de la crypto) suivant le performance predite pour la prochaine echeance

***  Mise en production
Faire une API pour tester le modèle de ML chosi et pourquoi pas requêter les données historique
Dockeriser les modules (partie streaming et API)  pour qu’il soit reproduisible sur n’importe quel machine

***  Automatisation des flux et Monitoring
automatiser à l’aide de DAG  d'Airflow  la récupération, la transformation et le stockage des données historiques . C'est donc créer un pipeline ETL. lors des differents taches du DAG, les données intermédiaires sont stockées sur un compartiment S3 d'AWS  


