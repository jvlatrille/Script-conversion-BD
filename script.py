import os
import time
import pandas as pd
import mysql.connector

# Codes de couleur ANSI pour la console
RESET = "\033[0m"
GREEN = "\033[92m"
ORANGE = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BLEU = "\033[94m"

# Connexion à la BD
conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="omdb"
)
curseur = conn.cursor()

# Exécute le script de création de tables
with open("tables.sql", "r") as f:
    creation_script = f.read()
curseur.execute(creation_script, multi=True)


# Fonction pour récupérer les colonnes d'une table
def obtenir_colonnes(curseur, nom_table):
    curseur.execute(f"SHOW COLUMNS FROM `{nom_table}`")
    return [col[0] for col in curseur.fetchall()]


# Fonction pour vérifier si une ligne existe déjà dans la table
def ligne_existe(curseur, nom_table, row_data, colonnes_table):
    where_clause = " AND ".join([f"`{col}` = %s" for col in colonnes_table])
    requete_verif = f"SELECT 1 FROM `{nom_table}` WHERE {where_clause} LIMIT 1"
    curseur.execute(requete_verif, tuple(row_data))
    return curseur.fetchone() is not None


# Fonction pour le nettoyage des valeurs (correction de certains formats du csv non compatibles)
def nettoyer_valeur(valeur):
    if pd.isnull(valeur) or valeur == "\\N":
        return None  # Remplace les valeurs nulles ou '\N' par None
    if isinstance(valeur, str) and "-" in valeur:
        try:
            return pd.to_datetime(valeur).strftime("%Y-%m-%d")  # Formate les dates
        except ValueError:
            return None
    return valeur


# Fonction pour ajouter des espaces tous les trois chiffres pour la lisibilité
def format_nombre(n):
    return f"{n:,}".replace(",", " ")


# Fonction pour le compte à rebours coloré (parce que c'est joli)
def compte_a_rebours():
    print(f"{CYAN}3{RESET}")
    time.sleep(1)
    print(f"{ORANGE}2{RESET}")
    time.sleep(1)
    print(f"{RED}1{RESET}")
    time.sleep(1)
    os.system("cls" if os.name == "nt" else "clear")


# Parcours des fichiers CSV
chemin_dossier = "."
for nom_fichier in os.listdir(chemin_dossier):
    if nom_fichier.endswith(".csv"):
        chemin_fichier = os.path.join(chemin_dossier, nom_fichier)
        nom_table = nom_fichier.replace(".csv", "")

        # Récupère les colonnes de la table
        colonnes_table = obtenir_colonnes(curseur, nom_table)

        # Lecture du CSV en ignorant les lignes mal formatées (on sais jamais)
        df = pd.read_csv(chemin_fichier, encoding="utf-8", on_bad_lines="skip")

        # Filtre les colonnes pour garder seulement celles présentes dans la table (normalement elles y sont toutes mais on sais jamais)
        df = df[[col for col in df.columns if col in colonnes_table]]

        # Prépare la requête d'insertion
        placeholders = ", ".join(["%s"] * len(df.columns))
        requete_insertion = f"INSERT IGNORE INTO `{nom_table}` ({', '.join(df.columns)}) VALUES ({placeholders})"

        erreurs = [] # Liste pour stocker les erreurs d'insertion
        compteur_erreurs = 0  # Compteur pour les erreurs dans cette table

        # On commence à insérer les données
        numero_ligne = 1
        for _, row in df.iterrows():
            row_data = [nettoyer_valeur(x) for x in row]

            try:
                # Vérifie si la ligne existe déjà dans la table
                if not ligne_existe(curseur, nom_table, row_data, df.columns):
                    curseur.execute(requete_insertion, tuple(row_data))
                    print(
                        f"{ORANGE}O Ligne {CYAN}{format_nombre(numero_ligne)}{ORANGE} dans la table {CYAN}{nom_table}{ORANGE} insérée{RESET}"
                    )
                else:
                    print(
                        f"{GREEN}X Ligne {CYAN}{format_nombre(numero_ligne)}{GREEN} dans la table {CYAN}{nom_table}{GREEN} déjà présente{RESET}"
                    )
            except mysql.connector.Error as e:
                # Stocke l'erreur dans la liste erreurs et augmente le compteur
                erreur_msg = f"Erreur ligne {format_nombre(numero_ligne)} dans table {nom_table}: \n{e}\n"
                erreurs.append(erreur_msg)
                compteur_erreurs += 1
                print(f"{RED}{erreur_msg}{RESET}")

            numero_ligne += 1

        # Commit pour enregistrer les modifications
        conn.commit()
        print(
            f"{BLEU}Toutes les données de {GREEN}{nom_fichier}{BLEU} insérées dans la table {nom_table}{RESET}"
        )

        # Affichage des erreurs pour cette table (s'il y en a)
        if erreurs:
            print(
                f"\n{RED}--- Récapitulatif des erreurs pour la table {nom_table} ---{RESET}"
            )
            for erreur in erreurs:
                print(f"{RED}{erreur}{RESET}")
            print(
                f"\n{RED}Total d'erreurs pour {nom_table} : {ORANGE}{format_nombre(compteur_erreurs)}{RESET}"
            )
            input(f"{CYAN}Appuyez sur Entrée pour continuer...{RESET}")
        else:
            print(
                f"{GREEN}Aucune erreur d'insertion pour la table {nom_table} !{RESET}"
            )

        # Compte à rebours de 3 secondes avant de passer à la table suivante
        compte_a_rebours()

# Fermeture de la connexion
curseur.close()
conn.close()

print(f"{GREEN}Processus terminé !{RESET}")
