import mysql.connector

# Connexion à la base de données MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="omdb"
)
cursor = conn.cursor()

# Liste étendue des catégories "normales" que l'on veut garder
valid_categories = [
    'Action', 'Comedy', 'Drama', 'Thriller', 'Horror', 'Adventure', 
    'Romance', 'Sci-Fi', 'Fantasy', 'Animation', 'Documentary',
    'Mystery', 'Biography', 'Crime', 'Family', 'Music', 'War', 'Western', 
    'Musical', 'Sport', 'History', 'Superhero'
]

# 1. Supprimer les films sortis avant 1990
delete_movies_before_1990 = """
DELETE FROM all_movies
WHERE date < '1990-01-01';
"""
cursor.execute(delete_movies_before_1990)
print("Films sortis avant 1990 supprimés.")

# 2. Supprimer les films qui n'ont pas de catégories valides
delete_invalid_category_movies = """
DELETE m FROM all_movies m
JOIN movie_categories mc ON m.id = mc.movie_id
JOIN category_names cn ON mc.category_id = cn.category_id
WHERE cn.name NOT IN (%s);
""" % (', '.join("'" + category + "'" for category in valid_categories))
cursor.execute(delete_invalid_category_movies)
print("Films sans catégories valides supprimés.")

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
        ("image_licenses", "image_id", "image_ids", "image_id")
    ]

    for table, foreign_key, parent_table, parent_key in tables_avec_fk:
        delete_orphaned_data = f"""
        DELETE FROM {table}
        WHERE {foreign_key} NOT IN (SELECT {parent_key} FROM {parent_table});
        """
        cursor.execute(delete_orphaned_data)
        print(f"Données orphelines supprimées dans {table} (basées sur {foreign_key} vers {parent_table}.{parent_key}).")

# Exécuter le nettoyage des données orphelines
nettoyer_donnees_orphelines(cursor)

# Validation des suppressions
conn.commit()
print("Nettoyage terminé et modifications enregistrées.")

# Fermeture de la connexion
cursor.close()
conn.close()
print("Connexion à la base de données fermée.")
