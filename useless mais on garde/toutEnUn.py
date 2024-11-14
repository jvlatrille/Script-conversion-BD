import os
import time
import pandas as pd
import mysql.connector
import datetime

# Codes de couleur ANSI pour la console
RESET = "\033[0m"
GREEN = "\033[92m"
ORANGE = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BLEU = "\033[94m"

# Connexion à la BD
conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="testbdvhs"
)
curseur = conn.cursor()

# Fonction pour récupérer les colonnes d'une table
def obtenir_colonnes(curseur, nom_table):
    curseur.execute(f"SHOW COLUMNS FROM `{nom_table}`")
    return [col[0] for col in curseur.fetchall()]

# Fonction pour vérifier si une ligne existe déjà dans la table
# Mise à jour pour éviter les erreurs de syntaxe
def ligne_existe(curseur, nom_table, row_data, colonnes_table):
    valeurs_non_nulles = [(col, row_data[idx]) for idx, col in enumerate(colonnes_table) if row_data[idx] is not None]
    if not valeurs_non_nulles:
        return False  # Pas de colonnes valides pour vérification

    where_clause = " AND ".join([f"`{col}` = %s" for col, _ in valeurs_non_nulles])
    requete_verif = f"SELECT 1 FROM `{nom_table}` WHERE {where_clause} LIMIT 1"
    valeurs = tuple([val for _, val in valeurs_non_nulles])
    curseur.execute(requete_verif, valeurs)
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

# Mapping manuel des fichiers CSV vers les tables correspondantes
mapping_csv_bd = {
    "all_categories.csv": "Tag",
    "all_people.csv": "Personne",
    "all_movies.csv": "OA",
    "all_casts.csv": "Collaborer",
    "all_votes.csv": "OA",
    "movie_details.csv": "OA"
    # Ajoutez ici les autres fichiers CSV pertinents
}

# Ordre d'insertion en respectant les dépendances
ordre_insertion = [
    "Tag",  # Table indépendante
    "Personne",  # Table indépendante
    "OA",  # Table qui peut dépendre de tags ou d'autres attributs
    "Collaborer"  # Dépend des OA et Personnes
]

# Parcours des fichiers CSV dans l'ordre des dépendances
chemin_dossier = "."
for nom_table in ordre_insertion:
    fichiers_csv = [f for f in mapping_csv_bd if mapping_csv_bd[f] == nom_table]
    for nom_fichier in fichiers_csv:
        chemin_fichier = os.path.join(chemin_dossier, nom_fichier)

        # Récupère les colonnes de la table
        colonnes_table = obtenir_colonnes(curseur, nom_table)

        # Lecture du CSV en ignorant les lignes mal formatées
        df = pd.read_csv(chemin_fichier, encoding="utf-8", on_bad_lines="skip")

        # Filtre les colonnes pour garder seulement celles présentes dans la table
        df = df[[col for col in df.columns if col in colonnes_table]]

        # Déduplication du DataFrame pour éviter les erreurs de doublons
        if nom_table == "Tag":
            df = df.drop_duplicates(subset=["nom"])

        # Prépare la requête d'insertion
        placeholders = ", ".join(["%s"] * len(df.columns))
        requete_insertion = f"INSERT INTO `{nom_table}` ({', '.join(df.columns)}) VALUES ({placeholders})"

        erreurs = []  # Liste pour stocker les erreurs d'insertion
        compteur_erreurs = 0  # Compteur pour les erreurs dans cette table

        # Début du timer pour l'importation de la table
        start_time = datetime.datetime.now()

        # On commence à insérer les données
        numero_ligne = 1
        for _, row in df.iterrows():
            row_data = [nettoyer_valeur(x) for x in row]

            # Vérification des dépendances si nécessaire
            if nom_table == "Collaborer":
                # Vérifier si la personne et l'OA existent avant d'insérer dans Collaborer
                id_oa = row_data[df.columns.get_loc("idOA")] if "idOA" in df.columns else None
                id_personne = row_data[df.columns.get_loc("idPersonne")] if "idPersonne" in df.columns else None
                if id_oa and not ligne_existe(curseur, "OA", [id_oa], ["idOA"]):
                    print(f"{RED}Ligne {numero_ligne}: OA avec id {id_oa} n'existe pas, insertion ignorée.{RESET}")
                    continue
                if id_personne and not ligne_existe(curseur, "Personne", [id_personne], ["idPersonne"]):
                    print(f"{RED}Ligne {numero_ligne}: Personne avec id {id_personne} n'existe pas, insertion ignorée.{RESET}")
                    continue

            try:
                # Vérifie si la ligne existe déjà dans la table
                if not ligne_existe(curseur, nom_table, row_data, df.columns):
                    curseur.execute(requete_insertion, tuple(row_data[:len(df.columns)]))
                    if numero_ligne % 1000 == 0:
                        print(
                            f"{ORANGE}O Ligne {CYAN}{format_nombre(numero_ligne)}{ORANGE} dans la table {CYAN}{nom_table}{ORANGE} insérée{RESET}"
                        )
                else:
                    if numero_ligne % 1000 == 0:
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

        # Fin du timer pour l'importation de la table
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        print(f"{BLEU}Importation terminée pour la table {nom_table} en {elapsed_time}{RESET}")

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