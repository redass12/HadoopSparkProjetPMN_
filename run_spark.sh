#!/bin/bash

# Lancer l'application Spark ici

# Exemple (remplace avec ta propre commande Spark) :
# spark-submit --class com.example.Main --master yarn --deploy-mode client HadoopSparkProjetPMN.jar
spark-submit \
  --class main.Main \
  --master local[*] \
  --deploy-mode client \
  HadoopSparkProjetPMN.jar
