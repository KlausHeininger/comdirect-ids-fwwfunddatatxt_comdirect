using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Diagnostics;


namespace FWWFundDataTXT_Comdirect
{

    // Exportproperties der einzelnen Dateien
    class ExportProperties
    {

        // Properties
        public string ExportPath { get; set; }
        public string ExportName { get; set; }
        public string ColumnDelimiter { get; set; }
        public string StoredProcedure { get; set; }
        public string DataBaseServer { get; set; }
        public string DataBase { get; set; }
        public string FileEncoding { get; set; }

        // Constructor
        public ExportProperties(ProgrammProperties pproperties, Int32 exportID)
        {

            string sqlStoredProcedure = "FWWFundDataTXT.getExportProperties";
            sqlStoredProcedure = sqlStoredProcedure + " " + pproperties.F_lieferung_id + ", " + exportID.ToString();



            //// Debug
            //Debug.WriteLine(Program.dataBaseServer);
            //Debug.WriteLine(Program.dataBase);
            //Debug.WriteLine(Program.user);
            //Debug.WriteLine(Program.pw);
            //Debug.WriteLine(sqlStoredProcedure);


            //// Debug




            DatabaseConnector dbConnector = new DatabaseConnector(pproperties.F_lieferung_id, pproperties.DataBaseServer, pproperties.DataBase, pproperties.User, pproperties.Pw, sqlStoredProcedure);
            SqlDataReader read = dbConnector.openDatabaseConnection();


            if (read.HasRows)
            {

                while (read.Read())
                {

                    ExportPath = read.GetString(0);
                    ExportName = read.GetString(1);
                    ColumnDelimiter = read.GetString(2);
                    StoredProcedure = read.GetString(3);
                    DataBaseServer = read.GetString(4);
                    DataBase = read.GetString(5);
                    FileEncoding = read.GetString(6);

                }

            }




            dbConnector.closeDataBaseConnection(read);



        }

    }




    public class ProgrammProperties
    {
        //Projektvariablen
        public string F_lieferung_id { get; set; }
        public string F_flizenzen_id { get; set; }
        public string DataBaseServer { get; set; }
        public string DataBase { get; set; }
        public string User { get; set; }
        public string Pw { get; set; }
        public string SqlStoredProcedure { get; set; }
        public Int32 RunError { get; set; }



        // Exportproperties der jeweiligen Lieferung
        public ProgrammProperties(string f_lieferung_id)
        {
            //Projektvariablen
            //DataBaseServer = "smartass1";
            F_lieferung_id = f_lieferung_id;
            F_flizenzen_id = Tools.getExportProperty(f_lieferung_id, "f_flizenzen_id");
            DataBaseServer = Tools.getExportProperty(f_lieferung_id, "server");
            DataBase = Tools.getExportProperty(f_lieferung_id, "datenbank");
            User = "sa";
            Pw = "gr#it";
            SqlStoredProcedure = Tools.getExportProperty(f_lieferung_id, "storedProcedure");
            RunError = 0;




        }


    }


    public static class Tools
    {

        public static string getExportProperty(string f_lieferung_id, string propertyType)
        {

            string DataBaseServer = "smartass1";
            string DataBase = "app_fundlisting";
            string User = "sa";
            string Pw = "gr#it"; //Muss durch integrierte Sicherheit ersetzt werden
            string sqlStoredProcedure = "lieferungen.getExportProperties";

            // Rückgabewert
            string property = string.Empty;

            sqlStoredProcedure = sqlStoredProcedure + " " + f_lieferung_id + ", [" + propertyType + "]";

            DatabaseConnector dbConnector = new DatabaseConnector(f_lieferung_id, DataBaseServer, DataBase, User, Pw, sqlStoredProcedure);
            SqlDataReader read = dbConnector.openDatabaseConnection();


            if (read.HasRows)
            {

                while (read.Read())
                {


                    property = read.GetString(0);

                }

            }

            dbConnector.closeDataBaseConnection(read);

            return property;

        }

        public static void SetLog(string f_lieferung_id, string fehlerID, string text)
        {

            string DataBaseServer = "smartass1";
            string DataBase = "app_fundlisting";
            string User = "sa";
            string Pw = "gr#it"; //Muss durch integrierte Sicherheit ersetzt werden
            string sqlStoredProcedure = "lieferungen.setLog";

            // Rückgabewert
            string property = string.Empty;

            DatabaseConnector dbConnector = new DatabaseConnector(f_lieferung_id, DataBaseServer, DataBase, User, Pw, sqlStoredProcedure);

            string DBConnection = "data source=" + DataBaseServer + ";initial catalog=" + DataBase + ";persist security info=False;user id=" + User + ";password=" + Pw + ";workstation id=automate;packet size=4096";
            SqlConnection conn = new SqlConnection();
            SqlCommand cmd = new SqlCommand();

            try
            {
                //Verbindung zur DB herstellen
                conn.ConnectionString = DBConnection;
                conn.Open();
                cmd.Connection = conn;

                //Daten in die DB spielen
                cmd.CommandText = sqlStoredProcedure + " " + "'" + f_lieferung_id + "','" + fehlerID + "','" + text + "'";
                //// Debug

                //Debug.WriteLine(cmd.CommandText);

                //// Debug
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {

                /* Exitcode auf "Fehler" setzen */
                Int32 ExitCode = 1;

                /* Pfad zum Errolog */
                String errorPfad = "ErrorLog.txt";

                /* Fehlermeldung in Textdatei schreiben, weil keine DB-Verbindung möglich ist */
                File.WriteAllText(errorPfad, f_lieferung_id + ", DB-nicht erreichbar, " + ex.Message);

                // Programm beenden
                Environment.Exit(ExitCode);

            }
            finally
            {
                //Datenbankverbindung schließen
                cmd.Dispose();
                conn.Close();

            }


        }

        public static void stop(string f_lieferung_id, string fehlerID, string text, Int32 ExitCode)
        {

            Tools.SetLog(f_lieferung_id, fehlerID, text);

            // Laufende Programmteile beenden





            // Programm beenden
            Environment.Exit(ExitCode);

        }


    }



}
