# GEOSIRENE-2019
<B>Téléchargement et insertion dans une table sql serveur de la base SIRENE Geocodée VERSION 2019</b>
<hr>
Le gouvernement français développe une politique d'ouverture des données publiques (OPEN DATA) auquel est dédié le site http://data.gouv.fr
<br><br>
Un des jeux de données d'intérêt majeur est constitué par la base SIRENE des entreprises et de leurs établissements  https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/'
<br>

Ce jeu de données a été géocodée par <a href="https://github.com/cquest">Christian Quest</a> à partir de la BAN (Base Adresse Nationale) et la BANO (Base Adresse Nationale Ouverte). <br>

 Le csv téléchargé comporte plus de 10 millions d'entitéés, et 91 colonnes : les 84 de la BD SIRENE originale + 7 ajoutées avec le géocodage, dont la latitude et la longitude en EPSG 4326).<br>

 Le script python relancé chaque mois pour réactualiser la base est disponible sur son github <br>

 Les colonnes de libellés de la BD SIRENE originale contenant de longues chaînes de caractères ont été éliminées pour gagner en mémoire. <br>

 
Christian Quest gère le raffraichissement du géocodage de la base SIRENE en open data et la met à disposition sur un repository à cette adresse
http://data.cquest.org/geo_sirene/last/ <br>
 Merci à Christian pour sa contribution permanente intelligente et utile !
 <hr>

Ce développement c# permet d'intégrer ce référentiel  dans une table sql serveur afin de réaliser facilement des traitements
<hr>
<b>Mise en oeuvre</b><br>
(1) Créer dans votre base sql serveur les tables avec la commande contenue dans le fichier create_table  <br>
(2) Ajuster dans le fichier de congiguration les variables (nom de votre base de données, credential pour se connecter, répertoire temporaire...)<br>
(3) Lancer le programme après compilation en ligne de commande<br>
(4) Créer si besoin des index dans votre table en fonction des traitements que vous souhaitez réaliser 
<br>
 