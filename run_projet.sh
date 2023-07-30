#!/bin/bash


function verification_input() {
    if [ "$(ls -A input)" ]; then
        echo "Le dossier input n'est pas vide."
    else
        echo "Le dossier input est vide."
    fi
}

function copie_dans_input() {
    cp chemin/vers/fichiers/source/*.csv input/
}


function copie_dans_tmp() {
    cp input/*.csv tmp/
}

function application_spark() {
    spark-submit --class Main --master local[*] chemin/vers/votre/projet.jar
}


function suppression_dossier_temporaire() {
    rm -rf tmp
}


function suppression_contenu_input() {
    rm -rf input/*
}


verification_input
copie_dans_input
copie_dans_tmp
application_spark
suppression_dossier_temporaire
suppression_contenu_input
