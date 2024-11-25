import os
import bz2
import shutil

def extract_bz2_files(source_folder, target_folder):
    # Vérifie que le dossier de destination existe, sinon le créer
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # Parcours tous les fichiers du dossier source
    for filename in os.listdir(source_folder):
        # Si le fichier est un .bz2
        if filename.endswith(".bz2"):
            source_path = os.path.join(source_folder, filename)
            target_path = os.path.join(target_folder, filename[:-4])  # Supprime l'extension .bz2 pour obtenir le nom du fichier extrait
            
            # Ouverture du fichier compressé
            with bz2.BZ2File(source_path, 'rb') as file:
                # Lecture des données décompressées
                data = file.read()
                
                # Écriture des données dans le fichier cible, écrasant s'il existe
                with open(target_path, 'wb') as target_file:
                    target_file.write(data)

if __name__ == "__main__":
    source_folder = "./compresses"  # Remplace par le chemin vers ton dossier "compressés"
    target_folder = "./decompresses"  # Dossier de destination pour les fichiers décompressés
    extract_bz2_files(source_folder, target_folder)
    print("Extraction terminée.")
