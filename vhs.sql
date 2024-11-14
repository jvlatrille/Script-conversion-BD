CREATE DATABASE IF NOT EXISTS testbdvhs;
USE testbdvhs;

-- Suppression des tables dans l'ordre des dépendances
DROP TABLE IF EXISTS vhs_porterSur;
DROP TABLE IF EXISTS vhs_fairePartie;
DROP TABLE IF EXISTS vhs_collaborer;
DROP TABLE IF EXISTS vhs_posseder;
DROP TABLE IF EXISTS vhs_constituer;
DROP TABLE IF EXISTS vhs_Tags;
DROP TABLE IF EXISTS vhs_Personne;
DROP TABLE IF EXISTS vhs_OA;
DROP TABLE IF EXISTS vhs_voir;
DROP TABLE IF EXISTS vhs_participer;
DROP TABLE IF EXISTS vhs_jouerpartie;
DROP TABLE IF EXISTS vhs_repondre;
DROP TABLE IF EXISTS vhs_question;
DROP TABLE IF EXISTS vhs_quizz;
DROP TABLE IF EXISTS vhs_collection;
DROP TABLE IF EXISTS vhs_notification;
DROP TABLE IF EXISTS vhs_message;
DROP TABLE IF EXISTS vhs_watchlist;
DROP TABLE IF EXISTS vhs_forum;
DROP TABLE IF EXISTS vhs_jeu;
DROP TABLE IF EXISTS vhs_utilisateur;


CREATE TABLE vhs_utilisateur
(

    idUtilisateur INT PRIMARY KEY,

    pseudo VARCHAR(50) NOT NULL,

    photoProfil VARCHAR(255),

    banniereProfil VARCHAR(255),

    adressMail VARCHAR(50) NOT NULL UNIQUE,

    motDePasse VARCHAR(200) NOT NULL,

    role VARCHAR(50) NOT NULL

);



CREATE TABLE vhs_jeu
(

    idJeu INT PRIMARY KEY,

    nom VARCHAR(50) NOT NULL,

    regle VARCHAR(255)

);



CREATE TABLE vhs_forum
(

    idForum INT PRIMARY KEY,

    nom VARCHAR(50) NOT NULL,

    description VARCHAR(255),

    theme VARCHAR(50),

    idUtilisateur INT,

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE SET NULL

);



CREATE TABLE vhs_watchlist
(

    idWatchlist INT PRIMARY KEY,

    titre VARCHAR(255) NOT NULL,

    genre VARCHAR(100),

    description VARCHAR(255),

    visible BOOLEAN DEFAULT FALSE,

    idUtilisateur INT NOT NULL,

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE

);



CREATE TABLE vhs_message
(

    idMessage INT PRIMARY KEY,

    contenu VARCHAR(255) NOT NULL,

    nbLike INT DEFAULT 0,

    nbDislike INT DEFAULT 0,

    idUtilisateur INT NOT NULL,

    idForum INT NOT NULL,

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE,

    FOREIGN KEY(idForum) REFERENCES vhs_forum(idForum) ON DELETE CASCADE

);



CREATE TABLE vhs_notification
(

    idNotif INT PRIMARY KEY,

    dateNotif DATE NOT NULL,

    destinataire VARCHAR(150),

    contenu VARCHAR(255),

    vu BOOLEAN DEFAULT FALSE,

    idUtilisateur INT NOT NULL,

    idJeu INT,

    idMessage INT,

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE,

    FOREIGN KEY(idJeu) REFERENCES vhs_jeu(idJeu) ON DELETE SET NULL,

    FOREIGN KEY(idMessage) REFERENCES vhs_message(idMessage) ON DELETE SET NULL

);



CREATE TABLE vhs_collection
(

    idCollection INT PRIMARY KEY,

    nom VARCHAR(100) NOT NULL,

    type VARCHAR(50),

    idCollectionParent INT,

    FOREIGN KEY (idCollectionParent) REFERENCES vhs_collection(idCollection) ON DELETE SET NULL

);



CREATE TABLE vhs_quizz
(

    idQuizz INT PRIMARY KEY,

    nom VARCHAR(100) NOT NULL,

    theme VARCHAR(50),

    nbQuestion INT,

    difficulte VARCHAR(25)

);



CREATE TABLE vhs_question
(

    idQuestion INT PRIMARY KEY,

    contenu VARCHAR(255) NOT NULL,

    numero INT NOT NULL,

    nvDifficulte VARCHAR(25),

    bonneReponse VARCHAR(150),

    idOa INT,

    FOREIGN KEY (idOa) REFERENCES vhs_oa(idOa) ON DELETE SET NULL

);



CREATE TABLE vhs_repondre
(

    idQuizz INT,

    idUtilisateur INT,

    score INT DEFAULT 0,

    PRIMARY KEY(idQuizz, idUtilisateur),

    FOREIGN KEY(idQuizz) REFERENCES vhs_quizz(idQuizz) ON DELETE CASCADE,

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE

);



CREATE TABLE vhs_jouerpartie
(

    idJeu INT,

    idUtilisateur INT,

    idUtilisateur2 INT,

    datePartie DATE,

    idJoueurGagnant INT,

    sujetDebat VARCHAR(255),

    PRIMARY KEY(idJeu, idUtilisateur, idUtilisateur2, datePartie),

    FOREIGN KEY(idJeu) REFERENCES vhs_jeu(idJeu) ON DELETE CASCADE,

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE,

    FOREIGN KEY(idUtilisateur2) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE

);



CREATE TABLE vhs_participer
(

    idForum INT,

    idUtilisateur INT,

    PRIMARY KEY(idForum, idUtilisateur),

    FOREIGN KEY(idForum) REFERENCES vhs_forum(idForum) ON DELETE CASCADE,

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE

);



CREATE TABLE vhs_voir
(

    idUtilisateur INT,

    idWatchlist INT,

    PRIMARY KEY(idUtilisateur, idWatchlist),

    FOREIGN KEY(idUtilisateur) REFERENCES vhs_utilisateur(idUtilisateur) ON DELETE CASCADE,

    FOREIGN KEY(idWatchlist) REFERENCES vhs_watchlist(idWatchlist) ON DELETE CASCADE

);



CREATE TABLE vhs_OA
(

    idOA INT PRIMARY KEY,

    nom VARCHAR(255) NOT NULL,

    note INT,

    type VARCHAR(255),

    description VARCHAR(255),

    dateSortie DATE,

    vo VARCHAR(255),

    duree INT

);



CREATE TABLE vhs_Personne
(

    IdPersonne INT PRIMARY KEY,

    nom VARCHAR(255) NOT NULL,

    prenom VARCHAR(255),

    dateNaiss DATE

);



CREATE TABLE vhs_Tags
(

    idTag INT PRIMARY KEY,

    nom VARCHAR(255) NOT NULL

);



CREATE TABLE vhs_constituer
(

    idWatchlist INT,

    idOA INT,

    PRIMARY KEY(idWatchlist, idOA),

    FOREIGN KEY(idWatchlist) REFERENCES vhs_watchlist(idWatchlist) ON DELETE CASCADE,

    FOREIGN KEY(idOA) REFERENCES vhs_OA(idOA) ON DELETE CASCADE

);



CREATE TABLE vhs_posseder
(

    idTag INT,

    idOA INT,

    PRIMARY KEY(idTag, idOA),

    FOREIGN KEY(idTag) REFERENCES vhs_Tags(idTag) ON DELETE CASCADE,

    FOREIGN KEY(idOA) REFERENCES vhs_OA(idOA) ON DELETE CASCADE

);



CREATE TABLE vhs_collaborer
(

    idPersonne INT,

    idOA INT,

    role VARCHAR(50),

    rang VARCHAR(50),

    PRIMARY KEY(idPersonne, idOA),

    FOREIGN KEY(idPersonne) REFERENCES vhs_Personne(idPersonne) ON DELETE CASCADE,

    FOREIGN KEY(idOA) REFERENCES vhs_OA(idOA) ON DELETE CASCADE

);



CREATE TABLE vhs_fairePartie
(

    idCollection INT,

    idOA INT,

    PRIMARY KEY(idCollection, idOA),

    FOREIGN KEY(idCollection) REFERENCES vhs_collection(idCollection) ON DELETE CASCADE,

    FOREIGN KEY(idOA) REFERENCES vhs_OA(idOA) ON DELETE CASCADE

);



CREATE TABLE vhs_porterSur
(

    idQuizz INT,

    idQuestion INT,

    PRIMARY KEY (idQuizz, idQuestion),

    FOREIGN KEY(idQuizz) REFERENCES vhs_quizz(idQuizz) ON DELETE CASCADE,

    FOREIGN KEY(idQuestion) REFERENCES vhs_question(idQuestion) ON DELETE CASCADE

); 