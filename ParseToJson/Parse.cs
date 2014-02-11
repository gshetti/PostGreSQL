using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Xml;
using System.Xml.Linq;
using System.Diagnostics;
using System.IO;
using System.Collections;

namespace ParseToJson
{
    class Parse
    {
        public void Logger(String lines, int flag = 0)
        {
            // Write the string to a file.append mode is enabled so that the log
            // lines get appended to  test.txt than wiping content and writing the log

            /* flag determines what to do with the error log
             0 --> (default) general error and written to errors.log
             1 -->  reports fileids of erroneous data*/

            try
            {
                if (!(Directory.Exists(@"../logs")))
                    Directory.CreateDirectory(@"../logs");
                System.IO.StreamWriter file;
                file = flag == 0 ? new System.IO.StreamWriter(@"../logs/errors.log", true) : new System.IO.StreamWriter(@"../logs/errors.log", true);
                file.WriteLine(lines);
                file.Close();
            }
            catch (Exception ex)
            {
                Console.Write("Error occured while writing to a log file. Check ..." + "\n" + ex.Message + "\n" + ex.StackTrace);
            }
        }


        static void Main(string[] args)
        {
            Parse obj = new Parse();

            try
            {
                if (args.Any())
                {
                    //read the filename from the input line
                    //all the files must be places in sql-dumps folder

                    string filepath = @"..\sql-dumps\" + args[0];
                    if (File.Exists(@filepath))
                        obj.ProcessFile(@filepath);
                    else
                        Console.WriteLine("Error finding the file. Enter a proper path/filename. Exiting ...");
                }
                else
                    Console.WriteLine("No filename specified .. Exiting");
            }

            catch (Exception ex)
            {
                obj.Logger("Error processing file : " + ex.Message + "\nStack trace : " + ex.StackTrace + "\n"); 
            }
        }


        void ProcessFile(string filename)
        {
            //timing to check the stats of the parse
            Stopwatch timePerParse;
            timePerParse = Stopwatch.StartNew();
            ConcurrentBag<Tuple<int, String>> finalResults = new ConcurrentBag<Tuple<int, String>>();
            

            Console.WriteLine("Reading the file into memory ...");


            using(StreamReader csvFile = new StreamReader(@filename))
            {
                string csvLine;
                int count = 0;
                List<string> lines = new List<string>();
                List<string> headerList = new List<string>() ;
                while ((csvLine = csvFile.ReadLine()) != null)
                {
                    if (count == 0)
                    {
                        Console.WriteLine("Processing the headers");
                        headerList = ProcessHeaders(csvLine); //processing all the headers for the file.
                    }
                    else
                    {
                        if (count % 100000 == 0)
                        {
                            //processing parallely the data for the file.
                            Parallel.ForEach(lines, line =>
                            {
                                var json_data = ConvertToJSON(line, headerList);
                                if (json_data != null)
                                {
                                    finalResults.Add(json_data);
                                }
                            });
                            Console.WriteLine(count.ToString() + " records processed");

                            // flush concurrent bag by writing to disk
                            WriteToJsonFile("censusRecord1940_json", finalResults);
                            finalResults = new ConcurrentBag<Tuple<int, String>>();
                            lines.Clear();
                        }
                        else
                        {
                            lines.Add(csvLine);
                        }

                    }
                    
                    count += 1;
                }

                if (lines.Count > 0)
                {
                    Parallel.ForEach(lines.Skip(1), line =>
                    {
                        var json_data = ConvertToJSON(line, headerList);
                        if (json_data != null)
                        {
                            finalResults.Add(json_data);
                        }
                    });
                    Console.WriteLine(count.ToString() + " records processed");

                    // flush concurrent bag by writing to disk
                    WriteToJsonFile("censusRecord1940_json", finalResults);
                    lines.Clear();
                }
            }
 
          timePerParse.Stop();
          Console.WriteLine("time to run the program : " + timePerParse.ElapsedTicks / Stopwatch.Frequency);   
        }

        private void WriteToJsonFile(string writeFile, ConcurrentBag<Tuple<int, String>> finalResults)
        {
            Console.Write("Writing to the file\n");
            try
            {
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"../" + writeFile))
                {
                    foreach (Tuple<int, string>  line in finalResults)
                    {
                        file.Write(line.Item1 + "|" + line.Item2 + "\n");
                    }
                    Console.WriteLine("Written to File\n");
                    file.Flush();
                    file.Dispose();
                }
            }
            catch (Exception ex)
            {
                Logger("Error writing to the  file : " + ex.Message + "\nStack trace : " + ex.StackTrace + "\n");
            }
        }
        
        ///<summary> ConvertToJson is a method in Parse Class. Primary function for this method is to jsonify the data</summary>
        ///<param name="line"> String which is pipe delimited</param>
        ///<param name="headerList">List of header field values</param>
        ///<returns>Tuple of type int: FileID and the headerlist of type List of string: json data</returns>
        ///
        private Tuple<int, String> ConvertToJSON(string line, List<string> headerList)
        {
            try
            {
                Dictionary<string, string> json = new Dictionary<string, string>();
                Dictionary<string, string> additionaljson = new Dictionary<string, string>();
                int i = 0;
                string[] data = line.Split('|');

                if (data.Length > headerList.Count)     //throw an exception since number of headers must match data points
                    throw new System.Exception("Fields on split do not match");

                foreach (string value in data)
                {
                    if (headerList[i].Equals(new StringBuilder("additional").ToString(), StringComparison.InvariantCultureIgnoreCase))
                        additionaljson = FlattenAdditionalFields(value.Substring(1));
                    else
                    {
                        if (String.Equals(value, ""))         //json structure needs to store null for emoty fields. It gets fed into postgres later
                            json.Add(headerList[i], null);
                        else
                            json.Add(headerList[i], value.Replace("\"", ""));
                    }

                    i += 1;
                }

                //merging the additional fields in json format to original json fields.
                foreach (var item in additionaljson)
                {
                    if (json.ContainsKey(item.Key))
                        Console.WriteLine("Dupliate key exists for : " + item.Key);
                    else
                        json.Add(item.Key, item.Value);
                }

                var jsonified_data = MyDictionaryToJson(json);      //convert dictonary to a json string
                return Tuple.Create(Convert.ToInt32(data[0]), jsonified_data);
            }
            catch (Exception ex)
            {
                Logger("Error processing a record : " + ex.Message + "\n stacktrace : " + ex.StackTrace + "\n Data: " + line);
                Logger(line.Split('|')[0], 1);
                return null;
            }
        }

        /// <summary>Method of class Parse. This flattens the data from additional fields and places them in a dictionary.</summary>
        /// <param name="data">data is a string in xml format</param>
        /// <returns>Returns a Dictionary of type string : headername. string: value</returns>
        private Dictionary<string, string> FlattenAdditionalFields(string data)
        {
            Dictionary<string, string> json = new Dictionary<string, string>();
            XDocument additionalXML = XDocument.Parse(data);
            foreach (XElement xe in additionalXML.Root.Descendants())
            {
                if (String.Equals(xe.Value, ""))        //json structure needs to store null for emoty fields. It gets fed into postgres later
                    json.Add(String.Concat("add_", xe.Name), null);
                else
                    json.Add(String.Concat("add_", xe.Name), xe.Value.Replace("\"", ""));
            }
            return json;
        }

        /// <summary> Stores the headers
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        List<string> ProcessHeaders(string data)
        {
            string[] headers = data.Split('|');
            List<string> headerList = new List<string>(headers);
            return headerList;
        }

        string MyDictionaryToJson(Dictionary<String, String> dict)
        {
            var entries = dict.Select(d =>
                string.Format("\"{0}\": \"{1}\"", d.Key, d.Value));
            return "{" + string.Join(",", entries) + "}";
        }
    }
}
