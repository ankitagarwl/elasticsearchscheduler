using Nest;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace elasticscheduler
{
    public class BAL
    {
        public void schdeulerInsertion()
        {

            WriteToFile("ElasticSearch Scheduler starts at {0}");
            ConnectionSettings connectionSettings = new ConnectionSettings(new Uri(ConfigurationManager.AppSettings["elasticsearchhost"])); //local PC            
            ElasticClient elasticClient = new ElasticClient(connectionSettings);

            try
            {
                string sourcepath_bslash = ConfigurationManager.AppSettings["sourcepath_bslash"]; //path
                string sourcepath_fslash = ConfigurationManager.AppSettings["sourcepath_fslash"];//path_tomove

                string rootFolderPath = sourcepath_fslash;
                string destinationPath = sourcepath_fslash + ConfigurationManager.AppSettings["destinationfoldername"];

                List<string> list = new List<string>();
                System.IO.DriveInfo di = new System.IO.DriveInfo(sourcepath_bslash);

                // Get the root directory and print out some information about it.
                System.IO.DirectoryInfo dirInfo = new System.IO.DirectoryInfo(sourcepath_fslash);

                // Get the files in the directory and print out some information about them.
                System.IO.FileInfo[] fileNames = dirInfo.GetFiles("*.*");


                foreach (System.IO.FileInfo fi in fileNames)
                {
                    list.Add(fi.Name.ToString());
                }


                int filecount = 0;
                foreach (string filename in list)
                {
                    var response = elasticClient.Search<dynamic>(s => s
                     .Index(ConfigurationManager.AppSettings["indexname"])
                      .Type(ConfigurationManager.AppSettings["documenttype"])
                     .Query(q => q.Term("path", sourcepath_bslash + filename)))
                     ;
                    foreach (var hit in response.Hits)
                    {

                        try
                        {
                            int linescount = TotalLines(rootFolderPath + filename);
                            if (linescount == response.HitsMetaData.Total)
                            {
                                filecount++;
                                File.Move(rootFolderPath + filename, destinationPath + filename);
                                WriteToFile("Files Moved : " + filename + Environment.NewLine);
                            }
                            else
                            {
                                WriteToFile("File Not Moved : " + filename + Environment.NewLine);
                                //rtxSearchResult.AppendText("File Not Moved : " + filename + Environment.NewLine);
                                break;
                            }
                        }
                        catch (Exception ex)
                        {
                            WriteToFile("Error Occured " + ex.ToString() + " {0}");
                        }

                        // rtxSearchResult.AppendText("Files Moved : " + filecount.ToString() + Environment.NewLine);
                        break;
                    }

                }
                WriteToFile("ElasticSearch Scheduler end at {0} . Files Moved : " + filecount.ToString() + Environment.NewLine);
                WriteToFile("---------------------------------------------------------------------------------");

            }
            catch (Exception ex)
            {
                WriteToFile("Error Occured " + ex.ToString() + " {0}");
                //response.StatusCode = HttpStatusCode.BadRequest;
                //return response;
            }
        }
        private static int TotalLines(string filePath)
        {
            using (StreamReader r = new StreamReader(filePath))
            {
                int i = 0;
                while (r.ReadLine() != null) { i++; }
                return i;
            }
        }

        private static void WriteToFile(string text)
        {
            string path = "D:\\es_scheduler.txt";
            using (StreamWriter writer = new StreamWriter(path, true))
            {
                writer.WriteLine(string.Format(text, DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss tt")));
                writer.Close();
            }
        }
    }
}
