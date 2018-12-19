
if (select count(*) From sys.tables where name = 'ExceptionLog')>0
	drop table ExceptionLog
go

CREATE TABLE ExceptionLog
(
ExceptionLogID Bigint not null identity(1,1),
FileData NVARCHAR(MAX),
ErrorDescription NVARCHAR(max),
SourceFileName NVARCHAR(256),
LoadDate datetime
)
GO
if (select count(*) From sys.tables where name = 'SireneStockUniteLegale')>0
	drop table SireneStockUniteLegale
go

CREATE TABLE SireneStockUniteLegale(
SireneStockUniteLegaleID bigint not null identity(1,1),
siren varchar(16),
statutdiffusionunitelegale varchar(8),
unitepurgeeunitelegale varchar(100),
datecreationunitelegale date,
sigleunitelegale varchar(32),
sexeunitelegale varchar(16),
prenom1unitelegale varchar(128),
prenom2unitelegale varchar(128),
prenom3unitelegale varchar(128),
prenom4unitelegale varchar(128),
prenomusuelunitelegale varchar(128),
pseudonymeunitelegale varchar(128),
identifiantassociationunitelegale varchar(16),
trancheeffectifsunitelegale varchar(8),
anneeeffectifsunitelegale varchar(128),
datederniertraitementunitelegale date,
nombreperiodesunitelegale varchar(128),
categorieentreprise varchar(16),
anneecategorieentreprise varchar(128),
datedebut date,
etatadministratifunitelegale varchar(8),
nomunitelegale varchar(128),
nomusageunitelegale varchar(128),
denominationunitelegale nvarchar(128),
denominationusuelle1unitelegale varchar(128),
denominationusuelle2unitelegale varchar(128),
denominationusuelle3unitelegale varchar(128),
categoriejuridiqueunitelegale varchar(1000),
activiteprincipaleunitelegale varchar(100),
nomenclatureactiviteprincipaleunitelegale varchar(100),
nicsiegeunitelegale varchar(128),
economiesocialesolidaireunitelegale varchar(128),
caractereemployeurunitelegale varchar(128),
SourceFileName NVARCHAR(256),
CONSTRAINT [PK_SireneStockUniteLegale] PRIMARY KEY CLUSTERED 
(
	[SireneStockUniteLegaleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
)

GO


if (select count(*) From sys.tables where name = 'StockEtablissement')>0
	drop table StockEtablissement
go


CREATE TABLE [dbo].[StockEtablissement](
	[StockEtablissementID] [bigint] IDENTITY(1,1) NOT NULL,
	siren varchar(16)
	,nic varchar(16)
	,siret varchar(16)
	,statutDiffusionEtablissement varchar(8)
	,dateCreationEtablissement date
	,trancheEffectifsEtablissement varchar(8)
	,anneeEffectifsEtablissement varchar(256)
	,activitePrincipaleRegistreMetiersEtablissement varchar(8)
	,dateDernierTraitementEtablissement date
	,etablissementSiege varchar(8)
	,nombrePeriodesEtablissement varchar(64)
	,complementAdresseEtablissement varchar(64)
	,numeroVoieEtablissement varchar(8)
	,indiceRepetitionEtablissement varchar(8)
	,typeVoieEtablissement varchar(8)
	,libelleVoieEtablissement varchar(128)
	,codePostalEtablissement varchar(8)
	,libelleCommuneEtablissement varchar(128)
	,libelleCommuneEtrangerEtablissement varchar(128)
	,distributionSpecialeEtablissement varchar(64)
	,codeCommuneEtablissement varchar(128)
	,codeCedexEtablissement varchar(16)
	,libelleCedexEtablissement varchar(100)
	,codePaysEtrangerEtablissement varchar(100)
	,libellePaysEtrangerEtablissement varchar(128)
	,complementAdresse2Etablissement varchar(64)
	,numeroVoie2Etablissement varchar(8)
	,indiceRepetition2Etablissement varchar(8)
	,typeVoie2Etablissement  varchar(8)
	,libelleVoie2Etablissement  varchar(128)
	,codePostal2Etablissement varchar(8)
	,libelleCommune2Etablissement varchar(128)
	,libelleCommuneEtranger2Etablissement varchar(128)
	,distributionSpeciale2Etablissement varchar(64)
	,codeCommune2Etablissement varchar(8)
	,codeCedex2Etablissement varchar(16)
	,libelleCedex2Etablissement varchar(128)
	,codePaysEtranger2Etablissement varchar(8)
	,libellePaysEtranger2Etablissement varchar(128)
	,dateDebut date
	,etatAdministratifEtablissement varchar(8)
	,enseigne1Etablissement varchar(64)
	,enseigne2Etablissement varchar(64)
	,enseigne3Etablissement varchar(64)
	,denominationUsuelleEtablissement varchar(128)
	,activitePrincipaleEtablissement varchar(64)
	,nomenclatureActivitePrincipaleEtablissement varchar(64)
	,caractereEmployeurEtablissement varchar(8)
	,longitude float
	,latitude float
	,geo_score float
	,geo_type varchar(64)
	,geo_adresse nvarchar(512)
	,geo_id nvarchar(512)
	,geo_ligne varchar(8)
	,geo_l4 varchar(64)
	,geo_l5 varchar(64), 
	SourceFileName nvarchar(512),
CONSTRAINT [PK_StockEtablissement] PRIMARY KEY CLUSTERED 
(
	[StockEtablissementID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
)

GO