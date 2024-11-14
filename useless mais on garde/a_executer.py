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

# Connexion à la base de données MySQL
conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="omdb"
)
curseur = conn.cursor()

# Exécute chaque commande du script de création de tables depuis table.sql
with open("tables.sql", "r") as f:
    creation_script = f.read()

# Sépare les commandes SQL par des points-virgules
commands = creation_script.split(";")

for command in commands:
    command = command.strip()
    if command:  # Ignore les commandes vides
        try:
            curseur.execute(command)
        except mysql.connector.Error as e:
            print(f"Erreur lors de l'exécution de la commande: {command}")
            print(f"Erreur MySQL: {e}")

# Liste étendue des catégories valides
valid_categories = [
    "Action",
    "Comedy",
    "Drama",
    "Thriller",
    "Horror",
    "Adventure",
    "Romance",
    "Sci-Fi",
    "Fantasy",
    "Animation",
    "Documentary",
    "Mystery",
    "Biography",
    "Crime",
    "Family",
    "Music",
    "War",
    "Western",
    "Musical",
    "Sport",
    "History",
    "Superhero",
]


# Fonction pour vérifier et nettoyer les valeurs de date, etc.
def nettoyer_valeur(valeur):
    if pd.isnull(valeur) or valeur == "\\N":
        return None
    if isinstance(valeur, str) and "-" in valeur:
        try:
            return pd.to_datetime(valeur).strftime("%Y-%m-%d")
        except ValueError:
            return None
    return valeur


# Fonction pour ajouter des espaces tous les trois chiffres pour la lisibilité
def format_nombre(n):
    return f"{n:,}".replace(",", " ")


# Fonction pour le compte à rebours coloré
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

        # Lecture du CSV en ignorant les lignes mal formatées et en forçant le chargement
        df = pd.read_csv(
            chemin_fichier, encoding="utf-8", on_bad_lines="skip", low_memory=False
        )

        # Filtre pour conserver uniquement les données de 1990 et après
        if "date" in df.columns:
            df = df[df["date"] >= "1990-01-01"]

        # Filtre pour conserver uniquement les films ayant des catégories valides
        if nom_table == "all_movies" and "category_id" in df.columns:
            df = df[df["category_id"].isin(valid_categories)]

        # Nettoie les valeurs dans le dataframe
        df = df.applymap(nettoyer_valeur)

        # Récupère les colonnes de la table
        curseur.execute(f"SHOW COLUMNS FROM `{nom_table}`")
        colonnes_table = [col[0] for col in curseur.fetchall()]
        df = df[[col for col in df.columns if col in colonnes_table]]

        # Prépare la requête d'insertion
        placeholders = ", ".join(["%s"] * len(df.columns))
        requete_insertion = f"INSERT IGNORE INTO `{nom_table}` ({', '.join(df.columns)}) VALUES ({placeholders})"

        erreurs = []  # Liste pour stocker les erreurs d'insertion
        compteur_erreurs = 0

        # Insertion des données
        numero_ligne = 1
        for _, row in df.iterrows():
            row_data = [nettoyer_valeur(x) for x in row]

            # Vérifie si la ligne contient des valeurs None et ignore si c'est le cas
            if None in row_data:
                print(
                    f"{RED}Ignoré : ligne {CYAN}{format_nombre(numero_ligne)}{RED} dans {nom_table} (contient des valeurs NULL){RESET}"
                )
                numero_ligne += 1
                continue

            try:
                curseur.execute(requete_insertion, tuple(row_data))
                print(
                    f"{ORANGE}O Ligne {CYAN}{format_nombre(numero_ligne)}{ORANGE} dans la table {CYAN}{nom_table}{ORANGE} insérée{RESET}"
                )
            except mysql.connector.Error as e:
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

        # Affichage des erreurs pour cette table
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

        # Compte à rebours
        compte_a_rebours()


# Fonction pour nettoyer les données orphelines de façon dynamique
def nettoyer_donnees_orphelines(cursor):
    tables_avec_fk = [
        ("movie_categories", "movie_id", "all_movies", "id"),
        ("all_casts", "movie_id", "all_movies", "id"),
        ("all_casts", "person_id", "all_people", "id"),
        ("all_votes", "movie_id", "all_movies", "id"),
        ("movie_languages", "movie_id", "all_movies", "id"),
        ("movie_countries", "movie_id", "all_movies", "id"),
        ("movie_details", "movie_id", "all_movies", "id"),
        ("movie_references", "movie_id", "all_movies", "id"),
        ("movie_references", "referenced_id", "all_movies", "id"),
        ("movie_abstracts_fr", "movie_id", "all_movies", "id"),
        ("movie_content_updates", "movie_id", "all_movies", "id"),
        ("all_people_aliases", "person_id", "all_people", "id"),
        ("people_links", "people_id", "all_people", "id"),
        ("trailers", "movie_id", "all_movies", "id"),
        ("movie_links", "movie_id", "all_movies", "id"),
        ("image_licenses", "image_id", "image_ids", "image_id"),
    ]

    for table, foreign_key, parent_table, parent_key in tables_avec_fk:
        delete_orphaned_data = f"""
        DELETE FROM {table}
        WHERE {foreign_key} NOT IN (SELECT {parent_key} FROM {parent_table});
        """
        cursor.execute(delete_orphaned_data)
        print(
            f"Données orphelines supprimées dans {table} (basées sur {foreign_key} vers {parent_table}.{parent_key})."
        )


# Exécuter le nettoyage des données orphelines
nettoyer_donnees_orphelines(curseur)

# Validation des suppressions
conn.commit()
print("Nettoyage terminé et modifications enregistrées.")

# Fermeture de la connexion
curseur.close()
conn.close()

print(f"{GREEN}Processus terminé !{RESET}")
