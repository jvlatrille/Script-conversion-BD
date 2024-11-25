import os
import pandas as pd

# Dossier contenant les fichiers CSV
directory = "."

# Mapping des colonnes d'identifiant par fichier
id_columns_map = {
    "all_casts.csv": ["movie_id", "person_id", "job_id"],
    "all_categories.csv": ["id", "parent_id", "root_id"],
    "all_characters.csv": ["id"],
    "all_episodes.csv": ["id", "parent_id", "series_id"],
    "all_games.csv": ["id", "parent_id"],
    "all_movie_aliases_iso.csv": ["movie_id"],
    "all_movies.csv": ["id", "parent_id"],
    "all_movieseries.csv": ["id", "parent_id"],
    "all_people_aliases.csv": ["person_id"],
    "all_people.csv": ["id"],
    "all_seasons.csv": ["id", "parent_id"],
    "all_series.csv": ["id", "parent_id"],
    "all_votes.csv": ["movie_id"],
    "category_names.csv": ["category_id"],
    "image_ids.csv": ["image_id", "object_id"],
    "image_licenses.csv": ["image_id"],
    "job_names.csv": ["job_id"],
    "movie_abstracts_fr.csv": ["movie_id"],
    "movie_categories.csv": ["movie_id", "category_id"],
    "movie_content_updates.csv": ["movie_id"],
    "movie_countries.csv": ["movie_id"],
    "movie_details.csv": ["movie_id"],
    "movie_keywords.csv": ["movie_id", "category_id"],
    "movie_languages.csv": ["movie_id"],
    "movie_links.csv": ["movie_id"],
    "movie_references.csv": ["movie_id", "referenced_id"],
    "people_links.csv": ["movie_id"],
    "trailers.csv": ["movie_id"],
}

# Fonction pour purger les CSV avec une suppression en cascade
def purge_csv_files():
    # Dictionnaire pour stocker les IDs à supprimer par colonne d'identifiant
    ids_to_remove = {col: set() for columns in id_columns_map.values() for col in columns}

    # Première passe : identifier les IDs avec des valeurs nulles
    for filename, id_columns in id_columns_map.items():
        filepath = os.path.join(directory, filename)
        if not os.path.isfile(filepath):
            continue

        # Chargement du fichier CSV
        df = pd.read_csv(filepath, encoding="utf-8", on_bad_lines="skip")

        # Vérifier pour chaque colonne d'identifiant
        for id_col in id_columns:
            if id_col in df.columns:
                null_rows = df[id_col].isin(['\\N', None, ''])
                ids_to_remove[id_col].update(df.loc[null_rows, id_col].dropna().tolist())
                # Supprime les lignes avec des valeurs nulles dans cette colonne d'identifiant
                df = df[~null_rows]

        # Sauvegarde temporaire (on réécrira encore après la deuxième passe)
        df.to_csv(filepath, index=False)

    # Deuxième passe : supprimer toutes les références aux IDs incomplets dans les autres fichiers
    for filename, id_columns in id_columns_map.items():
        filepath = os.path.join(directory, filename)
        if not os.path.isfile(filepath):
            continue

        # Charger le fichier CSV
        df = pd.read_csv(filepath, encoding="utf-8", on_bad_lines="skip")
        
        # Vérifie les colonnes pour les références d'IDs et supprime les lignes qui contiennent des IDs à supprimer
        for col in id_columns:
            if col in df.columns and col in ids_to_remove:
                df = df[~df[col].isin(ids_to_remove[col])]
        
        # Sauvegarde finale du fichier purgé
        df.to_csv(filepath, index=False)

    print("Purge des CSV terminée avec suppression en cascade !")

# Nettoyage des collections vides
def clean_empty_collections():
    filepath = os.path.join(directory, "all_categories.csv")
    if os.path.isfile(filepath):
        df = pd.read_csv(filepath, encoding="utf-8", on_bad_lines="skip")
        
        # Supprimer les collections sans parent ou enfants
        linked_ids = set(df["id"]).intersection(set(df["parent_id"].dropna()))
        df = df[df["id"].isin(linked_ids) | df["parent_id"].notna()]
        
        df.to_csv(filepath, index=False)
        print("Nettoyage des collections vides terminé !")

# Nettoyage des personnes non liées
def clean_unused_people():
    people_file = os.path.join(directory, "all_people.csv")
    casts_file = os.path.join(directory, "all_casts.csv")
    if os.path.isfile(people_file) and os.path.isfile(casts_file):
        people_df = pd.read_csv(people_file, encoding="utf-8", on_bad_lines="skip")
        casts_df = pd.read_csv(casts_file, encoding="utf-8", on_bad_lines="skip")
        
        # Récupérer les IDs des personnes utilisées dans les castings
        used_people_ids = set(casts_df["person_id"])
        people_df = people_df[people_df["id"].isin(used_people_ids)]
        
        people_df.to_csv(people_file, index=False)
        print("Nettoyage des personnes non liées terminé !")

# Suppression des films avec des notes nulles
def clean_movies_with_null_notes():
    votes_file = os.path.join(directory, "all_votes.csv")
    if os.path.isfile(votes_file):
        votes_df = pd.read_csv(votes_file, encoding="utf-8", on_bad_lines="skip")

        # Identifier les films avec des notes nulles
        null_votes = votes_df["vote_average"].isnull()
        null_movie_ids = votes_df.loc[null_votes, "movie_id"].tolist()

        # Supprimer les lignes des fichiers liés
        for filename, id_columns in id_columns_map.items():
            filepath = os.path.join(directory, filename)
            if not os.path.isfile(filepath):
                continue

            df = pd.read_csv(filepath, encoding="utf-8", on_bad_lines="skip")
            if "movie_id" in id_columns and "movie_id" in df.columns:
                df = df[~df["movie_id"].isin(null_movie_ids)]

            df.to_csv(filepath, index=False)
        
        print("Nettoyage des films avec des notes nulles terminé !")

# Appel des fonctions
purge_csv_files()
clean_empty_collections()
clean_unused_people()
clean_movies_with_null_notes()
