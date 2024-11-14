import mysql.connector

# Fonction pour exécuter le script SQL de création de la base et des tables
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

# Connexion à la base cible "testbdvhs"
target_conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="testbdvhs"
)

execute_sql_file("vhs.sql", target_conn)

# Connexion à la base source "omdb"
source_conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="omdb"
)
source_cursor = source_conn.cursor(dictionary=True)
target_cursor = target_conn.cursor()

# Vérifier les colonnes de la table source 'all_movies' pour s'assurer des noms corrects
def check_columns():
    source_cursor.execute("SHOW COLUMNS FROM all_movies")
    columns = source_cursor.fetchall()
    print("Colonnes disponibles dans 'all_movies':")
    for col in columns:
        print(col["Field"])  # On utilise 'Field' pour obtenir le nom de la colonne

# Appel de la fonction de vérification des colonnes
check_columns()

