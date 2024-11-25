import os
import pandas as pd
import mysql.connector

# Connexion à la base cible "testbdvhs"
target_conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="testbdvhs"
)


# Exécuter le fichier SQL pour créer les tables dans la base cible
def execute_sql_file(filename, connection):
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


execute_sql_file("vhs.sql", target_conn)

target_cursor = target_conn.cursor()


# Fonction pour transférer les données de 'all_movies.csv' vers 'vhs_OA' avec descriptions, durées, notes et langues
def transfer_oa():
    try:
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

    # Fusion des informations supplémentaires pour les films
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

    # Insertion groupée des films dans la base de données
    oa_data = []
    for _, row in merged_movies_df.iterrows():
        id_oa = row["id"]
        nom = row["name"]
        date_sortie = row["date"]
        # Si aucune description n'est présente, la ligne est ignorée
        # if pd.isna(row["abstract"]):
        #     continue
        description = row["abstract"]
        duree = row["runtime"] if pd.notna(row["runtime"]) else None
        note = row["vote_average"] if pd.notna(row["vote_average"]) else None
        vo = (
            row["language_iso_639_1"]
            if pd.notna(row["language_iso_639_1"])
            else "Langue non spécifiée"
        )

        oa_data.append((id_oa, nom, "Film", description, date_sortie, duree, note, vo))

    # Insertion des films
    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_OA (idOA, nom, type, description, dateSortie, duree, note, vo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            oa_data,
        )
        target_conn.commit()
        print("Films insérés avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des films : {err}")

    # --- Insertion des séries et épisodes ---

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

    oa_data_series = []
    for _, row in merged_episodes_df.iterrows():
        id_oa = row["id"]
        nom = row["name"]
        date_sortie = row["date"]
        # Si aucune description n'est présente, la ligne est ignorée
        if pd.isna(row["abstract"]):
            continue
        description = row["abstract"]
        duree = row["runtime"] if pd.notna(row["runtime"]) else 40
        note = row["vote_average"] if pd.notna(row["vote_average"]) else None
        vo = (
            row["language_iso_639_1"]
            if pd.notna(row["language_iso_639_1"])
            else "Langue non spécifiée"
        )

        oa_data_series.append(
            (id_oa, nom, "Série", description, date_sortie, duree, note, vo)
        )

    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_OA (idOA, nom, type, description, dateSortie, duree, note, vo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            oa_data_series,
        )
        target_conn.commit()
        print("Séries insérées avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des séries : {err}")

    print("Transfert des données des séries et des épisodes vers 'vhs_OA' terminé !")


def transfer_tags():
    try:
        category_names_df = pd.read_csv(
            "category_names.csv", encoding="utf-8", on_bad_lines="skip"
        )
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        return

    tags_data = []
    for _, row in category_names_df.iterrows():
        id_tag = row["category_id"]
        nom = row["name"]

        tags_data.append((id_tag, nom))

    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_Tags (idTag, nom)
            VALUES (%s, %s)
            """,
            tags_data,
        )
        target_conn.commit()
        print("Tags insérés avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des tags : {err}")


def associate_tags_with_oa():
    try:
        # Lire le fichier movie_categories.csv pour les liens entre movie_id et category_id
        movie_categories_df = pd.read_csv(
            "movie_categories.csv", encoding="utf-8", on_bad_lines="skip"
        )
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        return

    # Préparer les données pour les associations en convertissant les IDs en int (types Python standards)
    associations_data = []
    for _, row in movie_categories_df.iterrows():
        movie_id = int(row["movie_id"])  # Conversion en int
        category_id = int(row["category_id"])  # Conversion en int

        associations_data.append((category_id, movie_id))

    # Insérer les associations dans la table vhs_posseder
    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_posseder (idTag, idOA)
            VALUES (%s, %s)
            """,
            associations_data,
        )
        target_conn.commit()
        print("Associations des tags aux OA insérées avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des associations : {err}")


# Fonction pour transférer les personnes de 'all_people.csv' vers 'vhs_Personne'
def transfer_people():
    try:
        # Lire le fichier CSV des personnes
        people_df = pd.read_csv("all_people.csv", encoding="utf-8", on_bad_lines="skip")
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture du fichier CSV : {e}")
        return

    # Préparer les données pour l'insertion
    people_data = []
    for _, row in people_df.iterrows():
        id_personne = row["id"]
        nom = row["name"].split()[-1] if " " in row["name"] else row["name"]
        prenom = row["name"].split()[0] if " " in row["name"] else None
        date_naiss = row["birthday"] if pd.notna(row["birthday"]) else None
        people_data.append((id_personne, nom, prenom, date_naiss))

    # Insérer les données dans la table vhs_Personne
    try:
        target_cursor.executemany(
            """
            INSERT INTO vhs_Personne (idPersonne, nom, prenom, dateNaiss)
            VALUES (%s, %s, %s, %s)
            """,
            people_data,
        )
        target_conn.commit()
        print("Transfert des données de 'all_people.csv' vers 'vhs_Personne' terminé !")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des personnes : {err}")


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

    collaboration_data = []

    for _, row in collaborations_df.iterrows():
        id_oa = row["movie_id"]
        id_personne = row["person_id"]
        job_id = row["job_id"]

        # Utiliser le dictionnaire pour trouver le rôle, ou définir "Inconnu" si le job_id n'existe pas
        role = job_names_dict.get(job_id, "Inconnu")
        rang = row["position"] if "position" in row else None

        # Ajouter les données de collaboration
        collaboration_data.append((id_personne, id_oa, role, rang))

    # Insérer les collaborations dans la base de données
    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_collaborer (idPersonne, idOA, role, rang)
            VALUES (%s, %s, %s, %s)
            """,
            collaboration_data,
        )
        target_conn.commit()
        print("Transfert des collaborations vers 'vhs_collaborer' terminé !")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des collaborations : {err}")


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

    # Transfert des Sagas
    saga_data = []
    for _, row in sagas_df.iterrows():
        saga_data.append((row["id"], row["name"], "Saga"))

    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_collection (idCollection, nom, type)
            VALUES (%s, %s, %s)
            """,
            saga_data,
        )
        target_conn.commit()
        print("Sagas insérées avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des sagas : {err}")

    # Transfert des Séries et mise à jour des relations avec les Sagas
    series_data = []
    series_relations = []
    for _, row in series_df.iterrows():
        series_data.append((row["id"], row["name"], "Série"))
        if pd.notna(row["parent_id"]):
            series_relations.append((row["parent_id"], row["id"]))

    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_collection (idCollection, nom, type)
            VALUES (%s, %s, %s)
            """,
            series_data,
        )
        # Mise à jour des relations parent-enfant pour les séries
        target_cursor.executemany(
            """
            UPDATE vhs_collection
            SET idCollectionParent = %s
            WHERE idCollection = %s
            """,
            series_relations,
        )
        target_conn.commit()
        print("Séries et relations avec les Sagas insérées avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des séries ou des relations : {err}")

    # Transfert des Saisons et mise à jour des relations avec les Séries
    seasons_data = []
    seasons_relations = []
    for _, row in seasons_df.iterrows():
        seasons_data.append((row["id"], row["name"], "Saison"))
        if pd.notna(row["parent_id"]):
            seasons_relations.append((row["parent_id"], row["id"]))

    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_collection (idCollection, nom, type)
            VALUES (%s, %s, %s)
            """,
            seasons_data,
        )
        # Mise à jour des relations parent-enfant pour les saisons
        target_cursor.executemany(
            """
            UPDATE vhs_collection
            SET idCollectionParent = %s
            WHERE idCollection = %s
            """,
            seasons_relations,
        )
        target_conn.commit()
        print("Saisons et relations avec les Séries insérées avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des saisons ou des relations : {err}")

    # Transfert des Épisodes et mise à jour des relations avec les Saisons
    episodes_data = []
    episodes_relations = []
    for _, row in episodes_df.iterrows():
        episodes_data.append((row["id"], row["name"], "Épisode"))
        if pd.notna(row["parent_id"]):
            episodes_relations.append((row["parent_id"], row["id"]))

    try:
        target_cursor.executemany(
            """
            INSERT IGNORE INTO vhs_collection (idCollection, nom, type)
            VALUES (%s, %s, %s)
            """,
            episodes_data,
        )
        # Mise à jour des relations parent-enfant pour les épisodes
        target_cursor.executemany(
            """
            UPDATE vhs_collection
            SET idCollectionParent = %s
            WHERE idCollection = %s
            """,
            episodes_relations,
        )
        target_conn.commit()
        print("Épisodes et relations avec les Saisons insérés avec succès.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion des épisodes ou des relations : {err}")

    print(
        "Transfert des collections (sagas, séries, saisons, épisodes) et relations terminé !"
    )


# Appel des fonctions pour transférer les données vers les tables appropriées
transfer_oa()
transfer_tags()
associate_tags_with_oa()
transfer_people()
transfer_collaborations()
transfer_collections()

print("Fini :)")

# Suppression des OA avec des notes nulles
# try:
#     target_cursor.execute(
#         """
#         UPDATE vhs_OA
#         SET note = FLOOR(4 + RAND() * 6)
#         WHERE note IS NULL;
#         """
#     )
#     target_conn.commit()
#     print("Suppression des OA avec des notes nulles terminée.")
# except mysql.connector.Error as err:
#     print(f"Erreur lors de la suppression des OA avec des notes nulles : {err}")

# # Suppression des collaborations sans OA associée
# try:
#     target_cursor.execute(
#         """
#         DELETE FROM vhs_collaborer
#         WHERE idOA NOT IN (SELECT idOA FROM vhs_OA);
#         """
#     )
#     target_conn.commit()
#     print("Collaborations sans OA associée supprimées.")
# except mysql.connector.Error as err:
#     print(f"Erreur lors de la suppression des collaborations sans OA associée : {err}")

# # Suppression des tags dans 'vhs_posseder' sans OA associée
# try:
#     target_cursor.execute(
#         """
#         DELETE FROM vhs_posseder
#         WHERE idOA NOT IN (SELECT idOA FROM vhs_OA);
#         """
#     )
#     target_conn.commit()
#     print("Tags sans OA associée supprimés.")
# except mysql.connector.Error as err:
#     print(f"Erreur lors de la suppression des tags sans OA associée : {err}")

# # Suppression des collections sans OA associée
# try:
#     target_cursor.execute(
#         """
#         DELETE FROM vhs_collection
#         WHERE idCollection NOT IN (SELECT idOA FROM vhs_OA);
#         """
#     )
#     target_conn.commit()
#     print("Collections sans OA associée supprimées.")
# except mysql.connector.Error as err:
#     print(f"Erreur lors de la suppression des collections sans OA associée : {err}")

# # Suppression des participations sans OA associée
# try:
#     target_cursor.execute(
#         """
#         DELETE FROM vhs_participer
#         WHERE idOA NOT IN (SELECT idOA FROM vhs_OA);
#         """
#     )
#     target_conn.commit()
#     print("Participations sans OA associée supprimées.")
# except mysql.connector.Error as err:
#     print(f"Erreur lors de la suppression des participations sans OA associée : {err}")

# # Suppression des personnes non liées dans 'vhs_collaborer'
# try:
#     target_cursor.execute(
#         """
#         DELETE FROM vhs_personne
#         WHERE idPersonne NOT IN (SELECT idPersonne FROM vhs_collaborer);
#         """
#     )
#     target_conn.commit()
#     print("Personnes non liées supprimées.")
# except mysql.connector.Error as err:
#     print(f"Erreur lors de la suppression des personnes non liées : {err}")


# Fermeture des connexions
target_cursor.close()
target_conn.close()