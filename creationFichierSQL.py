import os
import pandas as pd
import mysql.connector

# Supprimer insertion.sql s'il existe pour démarrer avec un fichier vide
if os.path.exists("insertion.sql"):
    os.remove("insertion.sql")

# Ouvrir insertion.sql une seule fois au début pour toute la session d’écriture
output_file = open("insertion.sql", "w", encoding="utf-8")

# Connexion à la base cible "testbdvhs"
try:
    target_conn = mysql.connector.connect(
        host="localhost", user="root", password="", database="testbdvhs"
    )
    target_cursor = target_conn.cursor()

    # Exécuter le fichier SQL pour créer les tables dans la base de données (sans l'ajouter à insertion.sql)
    def execute_sql_file_in_db(filename, connection):
        with open(filename, "r") as file:
            sql_script = file.read()
        cursor = connection.cursor()
        for statement in sql_script.split(";"):
            if statement.strip():
                try:
                    cursor.execute(statement)
                except mysql.connector.Error as err:
                    print(f"Erreur lors de l'exécution de la commande SQL : {err}")
        connection.commit()
        print(f"Script {filename} exécuté avec succès !")
        cursor.close()

    # Appeler la fonction pour exécuter les commandes de création de tables uniquement dans la base de données
    execute_sql_file_in_db("vhs.sql", target_conn)

except mysql.connector.Error as err:
    print(f"Erreur de connexion à la base de données : {err}")
    target_conn = None
finally:
    # On ferme la connexion car elle est utilisée uniquement pour exécuter le script vhs.sql
    if target_conn:
        target_cursor.close()
        target_conn.close()


# Fonction pour échapper les chaînes de caractères dans SQL
def escape_sql_string(value):
    if isinstance(value, str):
        return value.replace("'", "''")
    return value


print(
    "Prêt pour l'insertion : vhs.sql exécuté, insertion.sql est ouvert pour écriture."
)


# Fonction pour transférer les données de 'all_movies.csv' vers 'vhs_OA' avec descriptions, durées, notes et langues
def transfer_oa():
    try:
        # Charger les fichiers CSV nécessaires
        movies_df = pd.read_csv("all_movies.csv", encoding="utf-8", on_bad_lines="skip")
        episodes_df = pd.read_csv(
            "all_episodes.csv", encoding="utf-8", on_bad_lines="skip"
        )
        abstracts_df = pd.read_csv(
            "movie_abstracts_fr.csv", encoding="utf-8", on_bad_lines="skip"
        )
        details_df = pd.read_csv(
            "movie_details.csv", encoding="utf-8", on_bad_lines="skip"
        )
        votes_df = pd.read_csv("all_votes.csv", encoding="utf-8", on_bad_lines="skip")
        languages_df = pd.read_csv(
            "movie_languages.csv", encoding="utf-8", on_bad_lines="skip"
        )
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture d'un fichier CSV : {e}")
        return

    # Fusion des informations pour les films
    merged_movies_df = pd.merge(
        movies_df, abstracts_df, how="left", left_on="id", right_on="movie_id"
    )
    merged_movies_df = pd.merge(
        merged_movies_df, details_df, how="left", left_on="id", right_on="movie_id"
    )
    merged_movies_df = pd.merge(
        merged_movies_df, votes_df, how="left", left_on="id", right_on="movie_id"
    )
    languages_dict = (
        languages_df.drop_duplicates(subset=["movie_id"])
        .set_index("movie_id")["language_iso_639_1"]
        .to_dict()
    )
    merged_movies_df["language_iso_639_1"] = merged_movies_df["id"].map(languages_dict)

    # Insertion des films dans le fichier SQL
    for _, row in merged_movies_df.iterrows():
        id_oa = row["id"]
        nom = row["name"].replace("'", "''")  # Échappement des apostrophes
        date_sortie = row["date"]
        description = (
            row["abstract"].replace("'", "''")
            if pd.notna(row["abstract"])
            else "Description non disponible"
        )
        duree = row["runtime"] if pd.notna(row["runtime"]) else "NULL"
        note = row["vote_average"] if pd.notna(row["vote_average"]) else "NULL"
        vo = (
            row["language_iso_639_1"]
            if pd.notna(row["language_iso_639_1"])
            else "Langue non spécifiée"
        )

        # Écrire la requête d'insertion pour chaque film dans le fichier insertion.sql
        output_file.write(
            f"INSERT IGNORE INTO vhs_OA (idOA, nom, type, description, dateSortie, duree, note, vo) "
            f"VALUES ({id_oa}, '{nom}', 'Film', '{description}', '{date_sortie}', {duree}, {note}, '{vo}');\n"
        )

    # Fusion et insertion des épisodes (séries)
    merged_episodes_df = pd.merge(
        episodes_df, abstracts_df, how="left", left_on="id", right_on="movie_id"
    )
    merged_episodes_df = pd.merge(
        merged_episodes_df, details_df, how="left", left_on="id", right_on="movie_id"
    )
    merged_episodes_df = pd.merge(
        merged_episodes_df, votes_df, how="left", left_on="id", right_on="movie_id"
    )
    merged_episodes_df["language_iso_639_1"] = merged_episodes_df["id"].map(
        languages_dict
    )

    for _, row in merged_episodes_df.iterrows():
        id_oa = row["id"]
        nom = row["name"].replace("'", "''")
        date_sortie = row["date"]
        description = (
            row["abstract"].replace("'", "''")
            if pd.notna(row["abstract"])
            else "Description non disponible"
        )
        duree = row["runtime"] if pd.notna(row["runtime"]) else 40
        note = row["vote_average"] if pd.notna(row["vote_average"]) else "NULL"
        vo = (
            row["language_iso_639_1"]
            if pd.notna(row["language_iso_639_1"])
            else "Langue non spécifiée"
        )

        # Écrire la requête SQL d'insertion pour chaque épisode dans le fichier insertion.sql
        output_file.write(
            f"INSERT IGNORE INTO vhs_OA (idOA, nom, type, description, dateSortie, duree, note, vo) "
            f"VALUES ({id_oa}, '{nom}', 'Série', '{description}', '{date_sortie}', {duree}, {note}, '{vo}');\n"
        )

    print(
        "Requêtes d'insertion pour les films et épisodes ajoutées dans 'insertion.sql' avec succès."
    )


def transfer_tags():
    try:
        # Lecture du fichier de catégories
        category_names_df = pd.read_csv(
            "category_names.csv", encoding="utf-8", on_bad_lines="skip"
        )
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        return

    # Insertion des tags dans le fichier insertion.sql
    for _, row in category_names_df.iterrows():
        id_tag = row["category_id"]
        nom = row["name"].replace("'", "''")  # Échapper les apostrophes

        # Écrire la requête d'insertion pour chaque tag dans le fichier insertion.sql
        output_file.write(
            f"INSERT IGNORE INTO vhs_Tags (idTag, nom) VALUES ({id_tag}, '{nom}');\n"
        )

    print(
        "Requêtes d'insertion pour les tags ajoutées dans 'insertion.sql' avec succès."
    )


def associate_tags_with_oa():
    try:
        # Lire le fichier movie_categories.csv pour les liens entre movie_id et category_id
        movie_categories_df = pd.read_csv(
            "movie_categories.csv", encoding="utf-8", on_bad_lines="skip"
        )
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        return

    # Insérer les associations tag-OA dans insertion.sql
    for _, row in movie_categories_df.iterrows():
        movie_id = int(row["movie_id"])
        category_id = int(row["category_id"])

        # Écrire la requête d'insertion pour chaque association directement dans output_file
        output_file.write(
            f"INSERT IGNORE INTO vhs_posseder (idTag, idOA) VALUES ({category_id}, {movie_id});\n"
        )

    print(
        "Requêtes d'insertion pour les associations tag-OA ajoutées dans 'insertion.sql' avec succès."
    )


def transfer_people():
    try:
        # Lecture du fichier CSV des personnes
        people_df = pd.read_csv("all_people.csv", encoding="utf-8", on_bad_lines="skip")
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        return

    for _, row in people_df.iterrows():
        id_personne = row["id"]
        nom = (
            escape_sql_string(row["name"].split()[-1])
            if " " in row["name"]
            else escape_sql_string(row["name"])
        )
        prenom = (
            escape_sql_string(row["name"].split()[0]) if " " in row["name"] else "NULL"
        )
        date_naiss = f"'{row['birthday']}'" if pd.notna(row["birthday"]) else "NULL"

        # Ajustement pour entourer prenom de guillemets simples
        prenom_value = f"'{prenom}'" if prenom != "NULL" else "NULL"

        # Créer la requête avec les valeurs échappées
        query = (
            f"INSERT INTO vhs_Personne (idPersonne, nom, prenom, dateNaiss) "
            f"VALUES ({id_personne}, '{nom}', {prenom_value}, {date_naiss});\n"
        )

        # Écrire dans le fichier insertion.sql
        output_file.write(query)

    print("Requêtes de transfert des personnes écrites avec succès dans insertion.sql.")


def transfer_collaborations():
    try:
        # Charger les fichiers CSV
        collaborations_df = pd.read_csv(
            "all_casts.csv", encoding="utf-8", on_bad_lines="skip"
        )
        job_names_df = pd.read_csv(
            "job_names.csv", encoding="utf-8", on_bad_lines="skip"
        )
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture d'un fichier CSV : {e}")
        return

    # Créer un dictionnaire pour lier chaque job_id à son nom
    job_names_dict = job_names_df.set_index("job_id")["name"].to_dict()

    # Ouvrir le fichier insertion.sql en mode ajout
    with open("insertion.sql", "a", encoding="utf-8") as f:
        for _, row in collaborations_df.iterrows():
            id_oa = row["movie_id"]
            id_personne = row["person_id"]
            job_id = row["job_id"]

            # Utiliser le dictionnaire pour trouver le rôle, ou définir "Inconnu" si le job_id n'existe pas
            role = escape_sql_string(job_names_dict.get(job_id, "Inconnu"))
            rang_sql = f"{row['position']}" if pd.notna(row["position"]) else "NULL"

            # Écrire chaque requête d'insertion dans le fichier
            f.write(
                f"INSERT IGNORE INTO vhs_collaborer (idPersonne, idOA, role, rang) "
                f"VALUES ({id_personne}, {id_oa}, '{role}', {rang_sql});\n"
            )

    print(
        "Requêtes d'insertion pour les collaborations ajoutées dans 'insertion.sql' avec succès."
    )


def transfer_collections():
    try:
        # Charger les fichiers CSV
        sagas_df = pd.read_csv(
            "all_movieseries.csv", encoding="utf-8", on_bad_lines="skip"
        )
        series_df = pd.read_csv("all_series.csv", encoding="utf-8", on_bad_lines="skip")
        seasons_df = pd.read_csv(
            "all_seasons.csv", encoding="utf-8", on_bad_lines="skip"
        )
        episodes_df = pd.read_csv(
            "all_episodes.csv", encoding="utf-8", on_bad_lines="skip"
        )
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture d'un fichier CSV : {e}")
        return

    # Ouvrir le fichier insertion.sql en mode ajout
    with open("insertion.sql", "a", encoding="utf-8") as f:

        # Transfert des Sagas
        for _, row in sagas_df.iterrows():
            id_collection = row["id"]
            nom = escape_sql_string(row["name"])
            f.write(
                f"INSERT IGNORE INTO vhs_collection (idCollection, nom, type) VALUES ({id_collection}, '{nom}', 'Saga');\n"
            )

        # Transfert des Séries et mise à jour des relations avec les Sagas
        for _, row in series_df.iterrows():
            id_collection = row["id"]
            nom = escape_sql_string(row["name"])
            f.write(
                f"INSERT IGNORE INTO vhs_collection (idCollection, nom, type) VALUES ({id_collection}, '{nom}', 'Série');\n"
            )
            # Vérifier si parent_id est un entier valide
            if pd.notna(row["parent_id"]) and str(row["parent_id"]).isdigit():
                parent_id = int(row["parent_id"])
                f.write(
                    f"UPDATE vhs_collection SET idCollectionParent = {parent_id} WHERE idCollection = {id_collection};\n"
                )

        # Transfert des Saisons et mise à jour des relations avec les Séries
        for _, row in seasons_df.iterrows():
            id_collection = row["id"]
            nom = escape_sql_string(row["name"])
            f.write(
                f"INSERT IGNORE INTO vhs_collection (idCollection, nom, type) VALUES ({id_collection}, '{nom}', 'Saison');\n"
            )
            # Vérifier si parent_id est un entier valide
            if pd.notna(row["parent_id"]) and str(row["parent_id"]).isdigit():
                parent_id = int(row["parent_id"])
                f.write(
                    f"UPDATE vhs_collection SET idCollectionParent = {parent_id} WHERE idCollection = {id_collection};\n"
                )

        # Transfert des Épisodes et mise à jour des relations avec les Saisons
        for _, row in episodes_df.iterrows():
            id_collection = row["id"]
            nom = escape_sql_string(row["name"])
            f.write(
                f"INSERT IGNORE INTO vhs_collection (idCollection, nom, type) VALUES ({id_collection}, '{nom}', 'Épisode');\n"
            )
            # Vérifier si parent_id est un entier valide
            if pd.notna(row["parent_id"]) and str(row["parent_id"]).isdigit():
                parent_id = int(row["parent_id"])
                f.write(
                    f"UPDATE vhs_collection SET idCollectionParent = {parent_id} WHERE idCollection = {id_collection};\n"
                )

    print(
        "Requêtes d'insertion pour les collections et leurs relations ajoutées dans 'insertion.sql' avec succès."
    )


# Appel des fonctions pour transférer les données vers les tables appropriées
transfer_oa()
transfer_tags()
associate_tags_with_oa()
transfer_people()
transfer_collaborations()
transfer_collections()

# Fermeture du fichier insertion.sql
output_file.close()

print("Fini :)")
