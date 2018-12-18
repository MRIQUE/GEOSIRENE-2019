using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Specialized;
using System.IO;
using System.Text.RegularExpressions;
using System.Net;
using SevenZipExtractor;
using System.Data;
using System.Data.SqlClient;
using System.IO.Compression;

namespace GeoSirene
{
    class Program
    {
        static string strConnectionSting = "Server=" + ConfigurationManager.AppSettings.Get("DBServer") +
                            ";Database=" + ConfigurationManager.AppSettings.Get("Database") +
                            ";Trusted_Connection=True;";
        static string strErrorFilePath = ConfigurationManager.AppSettings.Get("ErroredFilePath");
        static int intMemoryRows = Convert.ToInt32(ConfigurationManager.AppSettings.Get("InMemoryRows"));
        static void Main(string[] args)
        {
            string GeoSireneURL = ConfigurationManager.AppSettings.Get("GeoSireneURL"); //"http://data.cquest.org/geo_sirene/lastss/"; 
            string ArchiveFileDownloadPath = ConfigurationManager.AppSettings.Get("ArchiveFileDownloadPath");//@"D:\Personal\ETL\alacroix\FileList\";
            string ExtractFilePath = ConfigurationManager.AppSettings.Get("ExtractFilePath"); //@"D:\Personal\ETL\alacroix\FileList\";
            
            string html = ReadHtmlContentFromUrl(GeoSireneURL);
            if (html != "error")
            {
                string status = ExecuteTableTruncateSTMT("ExceptionLog");
                status = ExecuteTableTruncateSTMT("SireneStockUniteLegale");
                status = ExecuteTableTruncateSTMT("StockEtablissement");

                if (status != "error")
                {
                    string[] FileList = GetAllFileList(html);
                    int cnt = 0;
                    Console.Write("File download process has been started \n\n");
                    foreach (string FileName in FileList)
                    {
                        if (!String.IsNullOrEmpty(FileName))
                        {
                            int Index = FileName.LastIndexOf('.');
                            string FileExt = FileName.Substring(Index, FileName.Length - Index);
                            if (File.Exists(ArchiveFileDownloadPath + FileName))
                            {
                                File.Delete(ArchiveFileDownloadPath + FileName);
                            }
                            try
                            {
                                using (var client = new WebClient())
                                {                                   
                                    client.DownloadFile(GeoSireneURL + FileName, ArchiveFileDownloadPath + FileName);
                                    Console.Write("File " + FileName + " Hass been Downloaded successfully \n");
                                }

                                string ArchiveFullFilePath = ArchiveFileDownloadPath + FileName;
                                string csvFileName = unZipFile(ArchiveFullFilePath, ExtractFilePath, FileName.Replace(FileExt, ""), FileExt);
                                Console.Write("File " + FileName + " successfully extracted to " + csvFileName + " \n");
                                readGeoSireneFile(ExtractFilePath + csvFileName, FileName);
                                Console.Write("File " + csvFileName + " has been loaded into Database \n\n");
                            }
                            catch (Exception ex)
                            {
                                Console.Write("\n\nFollwoing Error has occured.\n" + ex.Message.ToString() + "\nPlease try again later.");
                                var userinput = Console.ReadLine();
                                break;
                            }
                        }
                        cnt++;
                        //if (cnt == 5)
                        //    break;
                    }
                    Console.Write("\n\nAll File has been loaded. Please Hit enter to finish.");
                    var userinput1 = Console.ReadLine();
                }
            }           
        }
        public static void GenerateErrorRecordFile(DataTable GeoSirene, string ErrorFileName)
        {
            string CurrentTime = DateTime.Now.ToString("MM-dd-yyyy");
            string ErrorFileFullPath = strErrorFilePath + ErrorFileName + CurrentTime + ".txt";
            if (File.Exists(ErrorFileFullPath))
            {
                File.Delete(ErrorFileFullPath);
            }

            using (StreamWriter sw = File.CreateText(ErrorFileFullPath))
            {
                foreach (DataRow row in GeoSirene.Rows)
                {
                    sw.WriteLine(row.ItemArray[0]);
                }
            }
        }
        public static string[] GetAllFileList(string html)
        {
            Regex regexFile = new Regex(@"(<a.*?>.*?</a>)", RegexOptions.Singleline);
            MatchCollection matchesFile = regexFile.Matches(html);
            int counter = 0;
            string[] FileList = new string[matchesFile.Count];
            foreach (Match match in matchesFile)
            {
                string value = match.Groups[1].Value;
                string t = Regex.Replace(value, @"\s*<.*?>\s*", "", RegexOptions.Singleline);
                value = t;
                if (value.Contains("StockUniteLegale_utf8.zip") || value.Contains("StockEtablissement_utf8_geo.csv.gz")) 
                {
                    
                        FileList[counter] = value;
                        counter++;
                    
                }
            }
            return FileList;
        }
        public static string ReadHtmlContentFromUrl(string url)
        {
            string html = string.Empty;
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (Stream stream = response.GetResponseStream())
                using (StreamReader reader = new StreamReader(stream))
                {
                    html = reader.ReadToEnd();
                }
            }
            catch (Exception ex)
            {
                Console.Write("Error: \n" + ex.Message.ToString() + "\nPlease try again later.");
                html = "error";
                var userinput = Console.ReadLine();
            }
            return html;
        }

        public static string unZipFile(string ArchiveFilePath, string Desnation, string FileName, string FileExt)
        {
            string ExtractedFileName = "";
            using (ArchiveFile archiveFile = new ArchiveFile(ArchiveFilePath))
            {                
                foreach (Entry entry in archiveFile.Entries)
                {
                    if (FileExt == ".gz")
                    {
                        Desnation = Desnation + FileName;
                        ExtractedFileName = FileName;
                    }
                    else
                    {
                        Desnation = Desnation + entry.FileName.ToString();
                        ExtractedFileName = entry.FileName.ToString();
                    }
                    if (File.Exists(Desnation))
                    {
                        File.Delete(Desnation);
                    }

                    MemoryStream memoryStream = new MemoryStream();

                    using (FileStream file = new FileStream(Desnation , FileMode.Create, FileAccess.Write))
                    {
                        entry.Extract(file);
                    }

                }
            }
            return ExtractedFileName;
        }

        private static void readGeoSireneFile(string filePath, string fileName)
        {
            System.Data.DataTable GeoSirene = new System.Data.DataTable();
            System.Data.DataTable ExceptionLog = new System.Data.DataTable();

            ExceptionLog.Columns.Add("FileData", typeof(String));
            ExceptionLog.Columns.Add("ErrorDescription", typeof(String));
            ExceptionLog.Columns.Add("SourceFileName", typeof(String));
            ExceptionLog.Columns.Add("LoadDate", typeof(DateTime));

            string DestinationTableName = "";
            string[] strFields = null;
            string[] strFieldsTypes = null;
            if (fileName.Contains("StockEtablissement"))
            {
                DestinationTableName = "StockEtablissement";
                strFields = "siren,nic,siret,statutDiffusionEtablissement,dateCreationEtablissement,trancheEffectifsEtablissement,anneeEffectifsEtablissement,activitePrincipaleRegistreMetiersEtablissement,dateDernierTraitementEtablissement,etablissementSiege,nombrePeriodesEtablissement,complementAdresseEtablissement,numeroVoieEtablissement,indiceRepetitionEtablissement,typeVoieEtablissement,libelleVoieEtablissement,codePostalEtablissement,libelleCommuneEtablissement,libelleCommuneEtrangerEtablissement,distributionSpecialeEtablissement,codeCommuneEtablissement,codeCedexEtablissement,libelleCedexEtablissement,codePaysEtrangerEtablissement,libellePaysEtrangerEtablissement,complementAdresse2Etablissement,numeroVoie2Etablissement,indiceRepetition2Etablissement,typeVoie2Etablissement,libelleVoie2Etablissement,codePostal2Etablissement,libelleCommune2Etablissement,libelleCommuneEtranger2Etablissement,distributionSpeciale2Etablissement,codeCommune2Etablissement,codeCedex2Etablissement,libelleCedex2Etablissement,codePaysEtranger2Etablissement,libellePaysEtranger2Etablissement,dateDebut,etatAdministratifEtablissement,enseigne1Etablissement,enseigne2Etablissement,enseigne3Etablissement,denominationUsuelleEtablissement,activitePrincipaleEtablissement,nomenclatureActivitePrincipaleEtablissement,caractereEmployeurEtablissement,longitude,latitude,geo_score,geo_type,geo_adresse,geo_id,geo_ligne,geo_l4,geo_l5,SourceFileName".Split(',');
                strFieldsTypes = "String,String,String,String,DateTime,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,DateTime,String,String,String,String,String,String,String,String,float,float,float,String,String,String,String,String,String,String".Split(',');
                GeoSirene.Columns.Add("siren", typeof(String));
                GeoSirene.Columns.Add("nic", typeof(String));
                GeoSirene.Columns.Add("siret", typeof(String));
                GeoSirene.Columns.Add("statutDiffusionEtablissement", typeof(String));
                GeoSirene.Columns.Add("dateCreationEtablissement", typeof(DateTime));
                GeoSirene.Columns.Add("trancheEffectifsEtablissement", typeof(String));
                GeoSirene.Columns.Add("anneeEffectifsEtablissement", typeof(String));
                GeoSirene.Columns.Add("activitePrincipaleRegistreMetiersEtablissement", typeof(String));
                GeoSirene.Columns.Add("dateDernierTraitementEtablissement", typeof(String));
                GeoSirene.Columns.Add("etablissementSiege", typeof(String));
                GeoSirene.Columns.Add("nombrePeriodesEtablissement", typeof(String));
                GeoSirene.Columns.Add("complementAdresseEtablissement", typeof(String));
                GeoSirene.Columns.Add("numeroVoieEtablissement", typeof(String));
                GeoSirene.Columns.Add("indiceRepetitionEtablissement", typeof(String));
                GeoSirene.Columns.Add("typeVoieEtablissement", typeof(String));
                GeoSirene.Columns.Add("libelleVoieEtablissement", typeof(String));
                GeoSirene.Columns.Add("codePostalEtablissement", typeof(String));
                GeoSirene.Columns.Add("libelleCommuneEtablissement", typeof(String));
                GeoSirene.Columns.Add("libelleCommuneEtrangerEtablissement", typeof(String));
                GeoSirene.Columns.Add("distributionSpecialeEtablissement", typeof(String));
                GeoSirene.Columns.Add("codeCommuneEtablissement", typeof(String));
                GeoSirene.Columns.Add("codeCedexEtablissement", typeof(String));
                GeoSirene.Columns.Add("libelleCedexEtablissement", typeof(String));
                GeoSirene.Columns.Add("codePaysEtrangerEtablissement", typeof(String));
                GeoSirene.Columns.Add("libellePaysEtrangerEtablissement", typeof(String));
                GeoSirene.Columns.Add("complementAdresse2Etablissement", typeof(String));
                GeoSirene.Columns.Add("numeroVoie2Etablissement", typeof(String));
                GeoSirene.Columns.Add("indiceRepetition2Etablissement", typeof(String));
                GeoSirene.Columns.Add("typeVoie2Etablissement", typeof(String));
                GeoSirene.Columns.Add("libelleVoie2Etablissement", typeof(String));
                GeoSirene.Columns.Add("codePostal2Etablissement", typeof(String));
                GeoSirene.Columns.Add("libelleCommune2Etablissement", typeof(String));
                GeoSirene.Columns.Add("libelleCommuneEtranger2Etablissement", typeof(String));
                GeoSirene.Columns.Add("distributionSpeciale2Etablissement", typeof(String));
                GeoSirene.Columns.Add("codeCommune2Etablissement", typeof(String));
                GeoSirene.Columns.Add("codeCedex2Etablissement", typeof(String));
                GeoSirene.Columns.Add("libelleCedex2Etablissement", typeof(String));
                GeoSirene.Columns.Add("codePaysEtranger2Etablissement", typeof(String));
                GeoSirene.Columns.Add("libellePaysEtranger2Etablissement", typeof(String));
                GeoSirene.Columns.Add("dateDebut", typeof(DateTime));
                GeoSirene.Columns.Add("etatAdministratifEtablissement", typeof(String));
                GeoSirene.Columns.Add("enseigne1Etablissement", typeof(String));
                GeoSirene.Columns.Add("enseigne2Etablissement", typeof(String));
                GeoSirene.Columns.Add("enseigne3Etablissement", typeof(String));
                GeoSirene.Columns.Add("denominationUsuelleEtablissement", typeof(String));
                GeoSirene.Columns.Add("activitePrincipaleEtablissement", typeof(String));
                GeoSirene.Columns.Add("nomenclatureActivitePrincipaleEtablissement", typeof(String));
                GeoSirene.Columns.Add("caractereEmployeurEtablissement", typeof(String));

                GeoSirene.Columns.Add("longitude", typeof(float));
                GeoSirene.Columns.Add("latitude", typeof(float));
                GeoSirene.Columns.Add("geo_score", typeof(float));
                GeoSirene.Columns.Add("geo_type", typeof(String));
                GeoSirene.Columns.Add("geo_adresse", typeof(String));
                GeoSirene.Columns.Add("geo_id", typeof(String));
                GeoSirene.Columns.Add("geo_ligne", typeof(String));
                GeoSirene.Columns.Add("geo_l4", typeof(String));
                GeoSirene.Columns.Add("geo_l5", typeof(String));
                GeoSirene.Columns.Add("SourceFileName", typeof(String));
            }
            else if (fileName.Contains("StockUniteLegale"))
            {
                DestinationTableName = "SireneStockUniteLegale";
                strFields = "siren,statutdiffusionunitelegale,unitepurgeeunitelegale,datecreationunitelegale,sigleunitelegale,sexeunitelegale,prenom1unitelegale,prenom2unitelegale,prenom3unitelegale,prenom4unitelegale,prenomusuelunitelegale,pseudonymeunitelegale,identifiantassociationunitelegale,trancheeffectifsunitelegale,anneeeffectifsunitelegale,datederniertraitementunitelegale,nombreperiodesunitelegale,categorieentreprise,anneecategorieentreprise,datedebut,etatadministratifunitelegale,nomunitelegale,nomusageunitelegale,denominationunitelegale,denominationusuelle1unitelegale,denominationusuelle2unitelegale,denominationusuelle3unitelegale,categoriejuridiqueunitelegale,activiteprincipaleunitelegale,nomenclatureactiviteprincipaleunitelegale,nicsiegeunitelegale,economiesocialesolidaireunitelegale,caractereemployeurunitelegale,SourceFileName".Split(',');
                strFieldsTypes = "String,String,String,DateTime,String,String,String,String,String,String,String,String,String,String,String,DateTime,String,String,String,DateTime,String,String,String,String,String,String,String,String,String,String,String,String,String,String".Split(',');
                GeoSirene.Columns.Add("siren", typeof(String));
                GeoSirene.Columns.Add("statutdiffusionunitelegale", typeof(String));
                GeoSirene.Columns.Add("unitepurgeeunitelegale", typeof(String));
                GeoSirene.Columns.Add("datecreationunitelegale", typeof(DateTime));
                GeoSirene.Columns.Add("sigleunitelegale", typeof(String));
                GeoSirene.Columns.Add("sexeunitelegale", typeof(String));
                GeoSirene.Columns.Add("prenom1unitelegale", typeof(String));
                GeoSirene.Columns.Add("prenom2unitelegale", typeof(String));
                GeoSirene.Columns.Add("prenom3unitelegale", typeof(String));
                GeoSirene.Columns.Add("prenom4unitelegale", typeof(String));
                GeoSirene.Columns.Add("prenomusuelunitelegale", typeof(String));
                GeoSirene.Columns.Add("pseudonymeunitelegale", typeof(String));
                GeoSirene.Columns.Add("identifiantassociationunitelegale", typeof(String));
                GeoSirene.Columns.Add("trancheeffectifsunitelegale", typeof(String));
                GeoSirene.Columns.Add("anneeeffectifsunitelegale", typeof(String));
                GeoSirene.Columns.Add("datederniertraitementunitelegale", typeof(DateTime));
                GeoSirene.Columns.Add("nombreperiodesunitelegale", typeof(String));
                GeoSirene.Columns.Add("categorieentreprise", typeof(String));
                GeoSirene.Columns.Add("anneecategorieentreprise", typeof(String));
                GeoSirene.Columns.Add("datedebut", typeof(DateTime));

                GeoSirene.Columns.Add("etatadministratifunitelegale", typeof(String));
                GeoSirene.Columns.Add("nomunitelegale", typeof(String));
                GeoSirene.Columns.Add("nomusageunitelegale", typeof(String));
                GeoSirene.Columns.Add("denominationunitelegale", typeof(String));
                GeoSirene.Columns.Add("denominationusuelle1unitelegale", typeof(String));
                GeoSirene.Columns.Add("denominationusuelle2unitelegale", typeof(String));
                GeoSirene.Columns.Add("denominationusuelle3unitelegale", typeof(String));
                GeoSirene.Columns.Add("categoriejuridiqueunitelegale", typeof(String));
                GeoSirene.Columns.Add("activiteprincipaleunitelegale", typeof(String));
                GeoSirene.Columns.Add("nomenclatureactiviteprincipaleunitelegale", typeof(String));

                GeoSirene.Columns.Add("nicsiegeunitelegale", typeof(String));
                GeoSirene.Columns.Add("economiesocialesolidaireunitelegale", typeof(String));
                GeoSirene.Columns.Add("caractereemployeurunitelegale", typeof(String));
                GeoSirene.Columns.Add("SourceFileName", typeof(String));
                
            }
            else
            {
                strFields = new string[0];
                strFieldsTypes = new string[0];
            }

            int intRowCounter = 0;
            Int64 TotalRowCounter = 0;
            Int64 FailedRowCounter = 0;
            Int64 PassedRowCounter = 0;
            string strLine = "";
            int intSkipHeader = 0;
            string[] strLineFields = null;

            StreamReader objStreamReader = null;
            try
            {
                objStreamReader = new StreamReader(filePath);
                while (!objStreamReader.EndOfStream)
                {
                    strLine = objStreamReader.ReadLine();
                    if (intSkipHeader == 0)
                    {
                        intSkipHeader = 1;
                        continue;
                    }
                    strLineFields = SplitCSV(strLine);
                    string a = "";
                    int errorOccured = 0;
                    DataRow GeoSirene_Row = GeoSirene.NewRow();
                    for (int i = 0; i < strFields.Length; i++)
                    {
                        try
                        {
                            if (strFields[i] == "SourceFileName")
                            {
                                GeoSirene_Row[strFields[i]] = fileName;
                            }
                            else
                            {
                                if (strLineFields[i].Trim() == "")
                                {
                                    if (strFieldsTypes[i] == "String")
                                        GeoSirene_Row[strFields[i]] = "";
                                    else
                                        GeoSirene_Row[strFields[i]] = DBNull.Value;
                                }
                                else
                                {
                                    GeoSirene_Row[strFields[i]] = strLineFields[i].Trim();
                                }

                                
                            }
                        }
                        catch(Exception ex)
                        {
                            DataRow ExceptionLog_Row = ExceptionLog.NewRow();
                            ExceptionLog_Row["FileData"] = strLine;
                            ExceptionLog_Row["ErrorDescription"] = ex.Message;
                            ExceptionLog_Row["SourceFileName"] = fileName;
                            ExceptionLog_Row["LoadDate"] = DateTime.Now;
                            ExceptionLog.Rows.Add(ExceptionLog_Row);
                            errorOccured = 1;
                            FailedRowCounter++;
                            break;
                        }
                        
                         
                    }
                    if (errorOccured==0)
                    {
                        GeoSirene.Rows.Add(GeoSirene_Row);
                        intRowCounter++;
                        PassedRowCounter++;
                    }
                       

                    
                    TotalRowCounter++;

                    if (intRowCounter >= intMemoryRows)
                    {

                        LoadDataIntoDB(GeoSirene,DestinationTableName);
                        intRowCounter = 0;
                        GeoSirene.Rows.Clear();
                        GC.Collect();
                        Console.Write("Rows Has been Loaded into database: "+ TotalRowCounter.ToString() +" in table "+DestinationTableName+" \n");
                    }

                }
                if (intRowCounter >= 0)
                {

                    LoadDataIntoDB(GeoSirene,DestinationTableName);
                    intRowCounter = 0;
                    GeoSirene.Rows.Clear();
                }
                objStreamReader.Close();

                if (FailedRowCounter > 0)
                {
                    GenerateErrorRecordFile(ExceptionLog, DestinationTableName);
                    LoadDataIntoDB(ExceptionLog, "ExceptionLog");
                }
                
            }
            catch (Exception ex)
            {
                objStreamReader.Close();
                throw ex;
            }
        }

   private static string[] SplitCSV(string input)
   {
       Regex csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
       List<string> list = new List<string>();
       string curr = null;
       foreach (Match match in csvSplit.Matches(input))
       {
           curr = match.Value;
           if (0 == curr.Length)
           {
               list.Add("");
           }

           list.Add(curr.TrimStart(','));
       }

       return list.ToArray<string>();
   }
        private static string ExecuteTableTruncateSTMT(string TableName){
            string queryString = "TRUNCATE TABLE " + TableName;
            try
            {
                using (SqlConnection connection = new SqlConnection(strConnectionSting))
                {
                    SqlCommand command = new SqlCommand(queryString, connection);
                    connection.Open();
                    command.ExecuteNonQuery();
                }
                queryString = "Ok";
            }
            catch(Exception ex)
            {
                Console.Write("Error: \n" + ex.Message.ToString() + "\nPlease try again later.");
                queryString = "error";
                var userinput = Console.ReadLine();
            }

            return queryString;
        }
        
        private static void ExecuteStroreProcedure(){
            using (var conn = new SqlConnection(strConnectionSting))
            using (var command = new SqlCommand("usp_LoadGeoSirene", conn)
            {
                CommandType = CommandType.StoredProcedure
            })
            {
                command.CommandTimeout = 1000;
                conn.Open();
                command.ExecuteNonQuery();
            }
   }
        private static void LoadDataIntoDB(DataTable GeoSirene, string DestinationTableName)
        {
            try
            {
                using (SqlConnection con = new SqlConnection(strConnectionSting))
                {
                    con.Open();
                    DataRow[] rowArray = GeoSirene.Select();

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con))
                    {
                        bulkCopy.DestinationTableName = DestinationTableName;// "StockEtablissement";
                        bulkCopy.BulkCopyTimeout = 3600;
                        try
                        {
                            if (DestinationTableName == "StockEtablissement")
                            {
                                bulkCopy.ColumnMappings.Add("siren", "siren");
                                bulkCopy.ColumnMappings.Add("nic", "nic");
                                bulkCopy.ColumnMappings.Add("siret", "siret");
                                bulkCopy.ColumnMappings.Add("statutDiffusionEtablissement", "statutDiffusionEtablissement");
                                bulkCopy.ColumnMappings.Add("dateCreationEtablissement", "dateCreationEtablissement");
                                bulkCopy.ColumnMappings.Add("trancheEffectifsEtablissement", "trancheEffectifsEtablissement");
                                bulkCopy.ColumnMappings.Add("anneeEffectifsEtablissement", "anneeEffectifsEtablissement");
                                bulkCopy.ColumnMappings.Add("activitePrincipaleRegistreMetiersEtablissement", "activitePrincipaleRegistreMetiersEtablissement");
                                bulkCopy.ColumnMappings.Add("dateDernierTraitementEtablissement", "dateDernierTraitementEtablissement");
                                bulkCopy.ColumnMappings.Add("etablissementSiege", "etablissementSiege");
                                bulkCopy.ColumnMappings.Add("nombrePeriodesEtablissement", "nombrePeriodesEtablissement");
                                bulkCopy.ColumnMappings.Add("complementAdresseEtablissement", "complementAdresseEtablissement");
                                bulkCopy.ColumnMappings.Add("numeroVoieEtablissement", "numeroVoieEtablissement");
                                bulkCopy.ColumnMappings.Add("indiceRepetitionEtablissement", "indiceRepetitionEtablissement");
                                bulkCopy.ColumnMappings.Add("typeVoieEtablissement", "typeVoieEtablissement");
                                bulkCopy.ColumnMappings.Add("libelleVoieEtablissement", "libelleVoieEtablissement");
                                bulkCopy.ColumnMappings.Add("codePostalEtablissement", "codePostalEtablissement");
                                bulkCopy.ColumnMappings.Add("libelleCommuneEtablissement", "libelleCommuneEtablissement");
                                bulkCopy.ColumnMappings.Add("libelleCommuneEtrangerEtablissement", "libelleCommuneEtrangerEtablissement");
                                bulkCopy.ColumnMappings.Add("distributionSpecialeEtablissement", "distributionSpecialeEtablissement");
                                bulkCopy.ColumnMappings.Add("codeCommuneEtablissement", "codeCommuneEtablissement");
                                bulkCopy.ColumnMappings.Add("codeCedexEtablissement", "codeCedexEtablissement");
                                bulkCopy.ColumnMappings.Add("libelleCedexEtablissement", "libelleCedexEtablissement");
                                bulkCopy.ColumnMappings.Add("codePaysEtrangerEtablissement", "codePaysEtrangerEtablissement");
                                bulkCopy.ColumnMappings.Add("libellePaysEtrangerEtablissement", "libellePaysEtrangerEtablissement");
                                bulkCopy.ColumnMappings.Add("complementAdresse2Etablissement", "complementAdresse2Etablissement");
                                bulkCopy.ColumnMappings.Add("numeroVoie2Etablissement", "numeroVoie2Etablissement");
                                bulkCopy.ColumnMappings.Add("indiceRepetition2Etablissement", "indiceRepetition2Etablissement");
                                bulkCopy.ColumnMappings.Add("typeVoie2Etablissement", "typeVoie2Etablissement");
                                bulkCopy.ColumnMappings.Add("libelleVoie2Etablissement", "libelleVoie2Etablissement");
                                bulkCopy.ColumnMappings.Add("codePostal2Etablissement", "codePostal2Etablissement");
                                bulkCopy.ColumnMappings.Add("libelleCommune2Etablissement", "libelleCommune2Etablissement");
                                bulkCopy.ColumnMappings.Add("libelleCommuneEtranger2Etablissement", "libelleCommuneEtranger2Etablissement");
                                bulkCopy.ColumnMappings.Add("distributionSpeciale2Etablissement", "distributionSpeciale2Etablissement");
                                bulkCopy.ColumnMappings.Add("codeCommune2Etablissement", "codeCommune2Etablissement");
                                bulkCopy.ColumnMappings.Add("codeCedex2Etablissement", "codeCedex2Etablissement");
                                bulkCopy.ColumnMappings.Add("libelleCedex2Etablissement", "libelleCedex2Etablissement");
                                bulkCopy.ColumnMappings.Add("codePaysEtranger2Etablissement", "codePaysEtranger2Etablissement");
                                bulkCopy.ColumnMappings.Add("libellePaysEtranger2Etablissement", "libellePaysEtranger2Etablissement");
                                bulkCopy.ColumnMappings.Add("dateDebut", "dateDebut");
                                bulkCopy.ColumnMappings.Add("etatAdministratifEtablissement", "etatAdministratifEtablissement");
                                bulkCopy.ColumnMappings.Add("enseigne1Etablissement", "enseigne1Etablissement");
                                bulkCopy.ColumnMappings.Add("enseigne2Etablissement", "enseigne2Etablissement");
                                bulkCopy.ColumnMappings.Add("enseigne3Etablissement", "enseigne3Etablissement");
                                bulkCopy.ColumnMappings.Add("denominationUsuelleEtablissement", "denominationUsuelleEtablissement");
                                bulkCopy.ColumnMappings.Add("activitePrincipaleEtablissement", "activitePrincipaleEtablissement");
                                bulkCopy.ColumnMappings.Add("nomenclatureActivitePrincipaleEtablissement", "nomenclatureActivitePrincipaleEtablissement");
                                bulkCopy.ColumnMappings.Add("caractereEmployeurEtablissement", "caractereEmployeurEtablissement");
                                bulkCopy.ColumnMappings.Add("longitude", "longitude");
                                bulkCopy.ColumnMappings.Add("latitude", "latitude");
                                bulkCopy.ColumnMappings.Add("geo_score", "geo_score");
                                bulkCopy.ColumnMappings.Add("geo_type", "geo_type");
                                bulkCopy.ColumnMappings.Add("geo_adresse", "geo_adresse");
                                bulkCopy.ColumnMappings.Add("geo_id", "geo_id");
                                bulkCopy.ColumnMappings.Add("geo_ligne", "geo_ligne");
                                bulkCopy.ColumnMappings.Add("geo_l4", "geo_l4");
                                bulkCopy.ColumnMappings.Add("geo_l5", "geo_l5");
                                bulkCopy.ColumnMappings.Add("SourceFileName", "SourceFileName");
                                bulkCopy.WriteToServer(rowArray);
                            }
                            else if (DestinationTableName == "SireneStockUniteLegale")
                            {
                                bulkCopy.ColumnMappings.Add("siren", "siren");
                                bulkCopy.ColumnMappings.Add("statutdiffusionunitelegale", "statutdiffusionunitelegale");
                                bulkCopy.ColumnMappings.Add("unitepurgeeunitelegale", "unitepurgeeunitelegale");
                                bulkCopy.ColumnMappings.Add("datecreationunitelegale", "datecreationunitelegale");
                                bulkCopy.ColumnMappings.Add("sigleunitelegale", "sigleunitelegale");
                                bulkCopy.ColumnMappings.Add("sexeunitelegale", "sexeunitelegale");
                                bulkCopy.ColumnMappings.Add("prenom1unitelegale", "prenom1unitelegale");
                                bulkCopy.ColumnMappings.Add("prenom2unitelegale", "prenom2unitelegale");
                                bulkCopy.ColumnMappings.Add("prenom3unitelegale", "prenom3unitelegale");
                                bulkCopy.ColumnMappings.Add("prenom4unitelegale", "prenom4unitelegale");
                                bulkCopy.ColumnMappings.Add("prenomusuelunitelegale", "prenomusuelunitelegale");
                                bulkCopy.ColumnMappings.Add("pseudonymeunitelegale", "pseudonymeunitelegale");
                                bulkCopy.ColumnMappings.Add("identifiantassociationunitelegale", "identifiantassociationunitelegale");
                                bulkCopy.ColumnMappings.Add("trancheeffectifsunitelegale", "trancheeffectifsunitelegale");
                                bulkCopy.ColumnMappings.Add("anneeeffectifsunitelegale", "anneeeffectifsunitelegale");
                                bulkCopy.ColumnMappings.Add("datederniertraitementunitelegale", "datederniertraitementunitelegale");

                                bulkCopy.ColumnMappings.Add("nombreperiodesunitelegale", "nombreperiodesunitelegale");
                                bulkCopy.ColumnMappings.Add("categorieentreprise", "categorieentreprise");
                                bulkCopy.ColumnMappings.Add("anneecategorieentreprise", "anneecategorieentreprise");
                                bulkCopy.ColumnMappings.Add("datedebut", "datedebut");
                                bulkCopy.ColumnMappings.Add("etatadministratifunitelegale", "etatadministratifunitelegale");
                                bulkCopy.ColumnMappings.Add("nomunitelegale", "nomunitelegale");
                                bulkCopy.ColumnMappings.Add("nomusageunitelegale", "nomusageunitelegale");
                                bulkCopy.ColumnMappings.Add("denominationunitelegale", "denominationunitelegale");

                                bulkCopy.ColumnMappings.Add("denominationusuelle1unitelegale", "denominationusuelle1unitelegale");
                                bulkCopy.ColumnMappings.Add("denominationusuelle2unitelegale", "denominationusuelle2unitelegale");
                                bulkCopy.ColumnMappings.Add("denominationusuelle3unitelegale", "denominationusuelle3unitelegale");
                                bulkCopy.ColumnMappings.Add("categoriejuridiqueunitelegale", "categoriejuridiqueunitelegale");
                                bulkCopy.ColumnMappings.Add("activiteprincipaleunitelegale", "activiteprincipaleunitelegale");
                                bulkCopy.ColumnMappings.Add("nomenclatureactiviteprincipaleunitelegale", "nomenclatureactiviteprincipaleunitelegale");
                                bulkCopy.ColumnMappings.Add("nicsiegeunitelegale", "nicsiegeunitelegale");
                                bulkCopy.ColumnMappings.Add("economiesocialesolidaireunitelegale", "economiesocialesolidaireunitelegale");

                                bulkCopy.ColumnMappings.Add("caractereemployeurunitelegale", "caractereemployeurunitelegale");
                                bulkCopy.ColumnMappings.Add("SourceFileName", "SourceFileName");
                                bulkCopy.WriteToServer(rowArray);
                            }
                            else if (DestinationTableName == "ExceptionLog")
                            {
                                bulkCopy.ColumnMappings.Add("FileData", "FileData");
                                bulkCopy.ColumnMappings.Add("ErrorDescription", "ErrorDescription");
                                bulkCopy.ColumnMappings.Add("SourceFileName", "SourceFileName");
                                bulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
                                bulkCopy.WriteToServer(rowArray);
                            }

                            
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }
                }

            }
            catch (Exception ex)
            {

                throw ex;
            }
            
        }

    }

}
