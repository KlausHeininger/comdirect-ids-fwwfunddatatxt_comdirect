using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Diagnostics;

namespace FWWFundDataTXT_Comdirect
{

    class Header
    {

        // Properties
        public List<string> HeaderList { get; set; }


        // Constructor
        public Header(ProgrammProperties pproperties, ExportProperties eproperties)
        {
            // create Stored Procedure
            string sqlStoredProcedure = eproperties.StoredProcedure + " " + pproperties.F_lieferung_id;

            DatabaseConnector dbConnector = new DatabaseConnector(pproperties.F_lieferung_id, eproperties.DataBaseServer, eproperties.DataBase, pproperties.User, pproperties.Pw, sqlStoredProcedure);
            SqlDataReader reader = dbConnector.openDatabaseConnection();



            List<string> headerList = new List<string>();


            // Rotate Rows
            for (int i = 0; i < reader.FieldCount; i++)
            {
                // Write to List
                headerList.Add(reader.GetName(i));
            }



            //// Only if is something to write
            //if (reader.HasRows)
            //{
            //// Rotate Lines
            //while (reader.Read())
            //{
            //    // Rotate Rows
            //    for (int i = 0; i < reader.FieldCount; i++)
            //    {
            //        // Write to List
            //        headerList.Add(reader.GetName(i));
            //    }

            //    // Only first Line
            //    break;


            //}



            //}


            // Close Connection to Database
            dbConnector.closeDataBaseConnection(reader);

            // Copy List to Property-List
            HeaderList = headerList;




        }

        public void writeHeader(ExportProperties properties, TextWriter txtWriter, Header liste)
        {

            // Rotate Headers
            for (int i = 0; i < liste.HeaderList.Count; i++)
            {
                // Write Headers to File
                txtWriter.Write(liste.HeaderList[i]);

                // Write Delimiter, if not last row
                if (i < liste.HeaderList.Count - 1)
                {
                    txtWriter.Write(properties.ColumnDelimiter);
                }
                else
                {
                    txtWriter.WriteLine();
                }

            }



        }




    }


    class Data
    {

        // Constructor
        public Data(ProgrammProperties pproperties, ExportProperties eproperties, TextWriter txtWriter)
        {

            string sqlStoredProcedure = eproperties.StoredProcedure + " " + pproperties.F_lieferung_id;

            DatabaseConnector dbConnector = new DatabaseConnector(pproperties.F_lieferung_id, eproperties.DataBaseServer, eproperties.DataBase, pproperties.User, pproperties.Pw, sqlStoredProcedure);
            SqlDataReader reader = dbConnector.openDatabaseConnection();

            // Write Data to File
            writeData(eproperties, txtWriter, reader);



            // Close Connection to Database
            dbConnector.closeDataBaseConnection(reader);


        }


        private void writeData(ExportProperties properties, TextWriter txtWriter, SqlDataReader reader)
        {

            
            int RowCount = 0;
            object FieldValue;
            string RowType = "S";

            // Only if is something to write
            if (reader.HasRows)
            {


                // Rotate Rows
                while (reader.Read())
                {


                    // Rotate Columns
                    for (int i = 0; i < reader.FieldCount; i++)
                    {

                        FieldValue = reader.GetValue(i);

                        // Write Data
                        //txtWriter.Write(reader.GetValue(i));
                        txtWriter.Write(FieldValue);
                        //Debug.WriteLine(reader.GetValue(i));


                        // RowType bestimmen
                        if (i == 0)
                        {
                            RowType = FieldValue.ToString();
                        }


                        // Erste und letzte Zeile sind kürzer
                        // Erste Zeile
                        if (RowType == "S" && i == 0)
                        {
                            txtWriter.Write(properties.ColumnDelimiter);

                        }

                        if (RowType == "S" && i == 1)
                        {
                            txtWriter.WriteLine();
                            break;

                        }

                        // Letzte Zeile
                        if (RowType == "E" && i == 0)
                        {
                            txtWriter.Write(properties.ColumnDelimiter);
                        }

                        if (RowType == "E" && i == 1)
                        {
                            txtWriter.WriteLine();
                            break;
                        }
                        
                        // Write Delimiter, if not last row
                        if (RowType == "D" && i < reader.FieldCount - 1)
                        {
                            txtWriter.Write(properties.ColumnDelimiter);
                        }
                        else if (RowType == "D" && i == reader.FieldCount - 1)
                        {
                            txtWriter.WriteLine();
                        }

                        

                    }


                    RowCount++;

                }



            }




        }




    }





}