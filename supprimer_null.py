import mysql.connector

# Connexion à la base de données MySQL
conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="omdb"
)
curseur = conn.cursor()

# Fonction pour supprimer les lignes contenant des valeurs NULL dans une table
def supprimer_lignes_nulles(table):
    # Récupérer les colonnes de la table
    curseur.execute(f"SHOW COLUMNS FROM `{table}`")
    colonnes = [col[0] for col in curseur.fetchall()]

    # Construire une requête SQL qui supprime les lignes avec des NULL
    conditions = " OR ".join([f"`{col}` IS NULL" for col in colonnes])
    requete_suppression = f"DELETE FROM `{table}` WHERE {conditions}"

    # Exécuter la requête
    curseur.execute(requete_suppression)
    # Afficher le nombre de lignes supprimées
    print(f"Lignes supprimées dans {table} contenant des valeurs NULL: {curseur.rowcount}")

# Récupérer toutes les tables de la base de données
curseur.execute("SHOW TABLES")
tables = [table[0] for table in curseur.fetchall()]

# Parcourir chaque table et supprimer les lignes avec des NULL
for table in tables:
    supprimer_lignes_nulles(table)

# Valider les suppressions
conn.commit()
print("Suppression des valeurs NULL terminée dans toutes les tables.")

# Fermeture de la connexion
curseur.close()
conn.close()
