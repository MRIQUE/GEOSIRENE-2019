# GEOSIRENE-2019
<B>T�l�chargement et insertion dans une table sql serveur de la base SIRENE Geocod�e VERSION 2019</b>
<hr>
Le gouvernement fran�ais d�veloppe une politique d'ouverture des donn�es publiques (OPEN DATA) auquel est d�di� le site http://data.gouv.fr
<br><br>
Un des jeux de donn�es d'int�r�t majeur est constitu� par la base SIRENE des entreprises et de leurs �tablissements  https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/'
<br>

Ce jeu de donn�es a �t� g�ocod�e par <a href="https://github.com/cquest">Christian Quest</a> � partir de la BAN (Base Adresse Nationale) et la BANO (Base Adresse Nationale Ouverte). <br>

 Le csv t�l�charg� comporte plus de 10 millions d'entit��s, et 91 colonnes : les 84 de la BD SIRENE originale + 7 ajout�es avec le g�ocodage, dont la latitude et la longitude en EPSG 4326).<br>

 Le script python relanc� chaque mois pour r�actualiser la base est disponible sur son github <br>

 Les colonnes de libell�s de la BD SIRENE originale contenant de longues cha�nes de caract�res ont �t� �limin�es pour gagner en m�moire. <br>

 
Christian Quest g�re le raffraichissement du g�ocodage de la base SIRENE en open data et la met � disposition sur un repository � cette adresse
http://data.cquest.org/geo_sirene/last/ <br>
 Merci � Christian pour sa contribution permanente intelligente et utile !
 <hr>

Ce d�veloppement c# permet d'int�grer ce r�f�rentiel  dans une table sql serveur afin de r�aliser facilement des traitements
<hr>
<b>Mise en oeuvre</b><br>
(1) Cr�er dans votre base sql serveur les tables avec la commande contenue dans le fichier create_table  <br>
(2) Ajuster dans le fichier de congiguration les variables (nom de votre base de donn�es, credential pour se connecter, r�pertoire temporaire...)<br>
(3) Lancer le programme apr�s compilation en ligne de commande<br>
(4) Cr�er si besoin des index dans votre table en fonction des traitements que vous souhaitez r�aliser 
<br>
 