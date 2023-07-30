#!/bin/bash

# Vérifier si le répertoire "input" est vide ou non

if [ "$(ls -A input)" ]; then
    echo "Le répertoire input n'est pas vide."
else
    echo "Le répertoire input est vide."
fi
