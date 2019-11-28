/*
_______________________________________________________________________________

Erstellt die CSV-Dateien für die Comdirect Gebühren-Lieferung

Command-Line Argument: f_lieferung_id
_______________________________________________________________________________

[v1.0] - [28.11.2019] - kh@fww.de - Klon des FWW-FundDataTXT angepasst für die Comdirect-Lieferung

 
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Diagnostics;

namespace FWWFundDataTXT_Comdirect
{
    class Program
    {


        //Projektvariablen
        //public static string dataBaseServer = "smartass1";
        //public static string dataBase = "app_fww";
        //public static string user = "sa";
        //public static string pw = "gr#it"; //Muss durch integrierte Sicherheit ersetzt werden
        //public static string path = @"D:\daten\lizenzen\fww\export\FWWFundDataXML\";
        //public static string fileName = "Testdatei.xml";

        public static string sqlStoredProcedure = string.Empty;
        //public static string f_flizenzen_id = string.Empty;


        static void Main(string[] args)
        {

            // Log Start
            Tools.SetLog(args[0], "0", "Programmstart");

            // Get Main-Connection-Properties
            ProgrammProperties pproperties = new ProgrammProperties(args[0]);


            //// Get Commandline-Arguments (f_flizenzen_id)
            //f_flizenzen_id = properties.F_flizenzen_id;

            // Build Stored Procedure for Properties
            sqlStoredProcedure = pproperties.SqlStoredProcedure + ' ' + pproperties.F_lieferung_id;

            // Connect to Database
            DatabaseConnector dbConnector = new DatabaseConnector(pproperties.F_lieferung_id, pproperties.DataBaseServer, pproperties.DataBase, pproperties.User, pproperties.Pw, sqlStoredProcedure);
            // Get Data
            SqlDataReader reader = dbConnector.openDatabaseConnection();

            // Create Files
            FileCreator ExportDatei = new FileCreator(pproperties, reader);

            // Close Connection to Database
            dbConnector.closeDataBaseConnection(reader);



            // End Program
            Tools.stop(args[0], "0", "Programmende", 0);


        }
    }

    public class FileCreator
    {
        // Variables
        private SqlDataReader reader;
        private string dataBaseServer;
        private string dataBase;
        private string user;
        private string pw;

        //private string path;
        //private string fileName;

        // Properties
        public string DataBaseServer
        {
            get
            {
                return dataBaseServer;
            }
        }

        public string DataBase
        {
            get
            {
                return dataBase;
            }
        }

        public string User
        {
            get
            {
                return user;
            }
        }

        public string Pw
        {
            get
            {
                return pw;
            }
        }

        // Constructor
        public FileCreator(ProgrammProperties pproperties, SqlDataReader _reader)
        {

            //// Set Variables
            //dataBaseServer = _dataBaseServer;
            //dataBase = _dataBase;
            //user = _user;
            //pw = _pw;

            reader = _reader;
            string path = "";


            // Rotate files

            if (reader.HasRows)
            {

                while (reader.Read())
                {

                    // Set File Properties
                    ExportProperties eproperties = new ExportProperties(pproperties, reader.GetInt32(0));
                    path = eproperties.ExportPath + eproperties.ExportName;

                    //Debug.WriteLine(path);

                    // Check if File exists
                    FileInfo checkFile = new FileInfo(path);
                    if (checkFile.Exists == true)
                    {
                        checkFile.Delete();

                    }




                    // Create File

                    /* standard-encoding */
                    Encoding code = Encoding.GetEncoding("utf-8");

                    //if (eproperties.FileEncoding == "utf-8")
                    //{
                    //    code = Encoding.GetEncoding("utf-8");
                    //}


                    /* Overwrite standard-encoding */
                    if (eproperties.FileEncoding == "ansi")
                    {
                        code = Encoding.GetEncoding(28591);
                    }


                    StreamWriter txtWriter = new StreamWriter(path, true, code);

                    ////Encoding encoding = "UTF8"; 
                    //if (eproperties.Encoding == "ansi")
                    //{
                    //    StreamWriter txtWriter = new StreamWriter(path, true, Encoding.Default);
                    //}
                    //else
                    //{
                    //    StreamWriter txtWriter = new StreamWriter(path, true, Encoding.UTF8);
                    //}

                    //// Write Header to File -> Kein Header in dieser Version
                    //Header liste = new Header(pproperties, eproperties);
                    //liste.writeHeader(eproperties, txtWriter, liste);



                    // Write Data to File
                    Data daten = new Data(pproperties, eproperties, txtWriter);




                    try
                    {
                        // Save File
                        txtWriter.Close();
                    }
                    catch (Exception ex)
                    {

                        Tools.stop(pproperties.F_lieferung_id, "1", ex.Message, 1);

                    }



                    // Cleanup
                    path = "";
                    //rowDelimiter = "";




                }







            }






        }





    }



    public class DatabaseConnector
    {

        //Properties
        private string f_lieferung_id;
        private string dataBase;
        private string dataBaseServer;
        private string user;
        private string pw; //Muss durch integrierte Sicherheit ersetzt werden
        private string sqlStoredProcedure;
        private SqlConnection con;


        public string F_lieferung_id
        {
            get
            {
                return f_lieferung_id;
            }

            set
            {
                f_lieferung_id = value;
            }

        }


        public string DataBase
        {
            get
            {
                return dataBase;
            }

            set
            {
                dataBase = value;
            }

        }

        public string DataBaseServer
        {
            get
            {
                return dataBaseServer;
            }

            set
            {
                dataBaseServer = value;
            }

        }

        public string User
        {
            get
            {
                return user;
            }

            set
            {
                user = value;
            }

        }

        public string Pw
        {
            get
            {
                return pw;
            }

            set
            {
                pw = value;
            }

        }

        public string SqlStoredProcedure
        {
            get
            {
                return sqlStoredProcedure;
            }

            set
            {
                sqlStoredProcedure = value;
            }

        }


        //public DatabaseConnector(string _dataBaseServer, string _dataBase, string _user, string _pw, string _sqlStoredProcedure)
        //{
        //    dataBaseServer = _dataBaseServer;
        //    dataBase = _dataBase;
        //    user = _user;
        //    pw = _pw;
        //    sqlStoredProcedure = _sqlStoredProcedure;
        //}

        public DatabaseConnector(string _f_lieferung_id, string _dataBaseServer, string _dataBase, string _user, string _pw, string _sqlStoredProcedure)
        {
            f_lieferung_id = _f_lieferung_id;
            dataBaseServer = _dataBaseServer;
            dataBase = _dataBase;
            user = _user;
            pw = _pw;
            sqlStoredProcedure = _sqlStoredProcedure;
        }


        public SqlDataReader openDatabaseConnection()
        {

            //Rückgabewert
            SqlDataReader reader;

            con = new SqlConnection();

            con.ConnectionString = "data source=" + DataBaseServer + ";initial catalog=" + DataBase + ";persist security info=False;user id=" + User + ";password=" + Pw + ";workstation id=smartass1;packet size=4096";



            SqlCommand cmd = new SqlCommand(sqlStoredProcedure, con);
            cmd.CommandTimeout = 0;
            try
            {
                con.Open();
                //Debug.WriteLine(cmd.CommandText);

            }
            catch (Exception ex)
            {
                Tools.stop(F_lieferung_id, "1", "Error " + ex.Message, 1);
            }
            reader = cmd.ExecuteReader();

            return reader;
        }

        public void closeDataBaseConnection(SqlDataReader reader)
        {

            reader.Close();
            reader.Dispose();
            con.Close();
            con.Dispose();
        }



    }



}
