using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Configuration;
using System.Threading.Tasks;
using System.Text;
using Microsoft.HBase.Client;
using org.apache.hadoop.hbase.rest.protobuf.generated;

namespace TweetSentimentWeb.Models
{
    public class HBaseReader
    {
        // For reading Tweet sentiment data from HDInsight HBase
        HBaseClient client;

        // HDinsight HBase cluster and HBase table information
        const string CLUSTERNAME = "https://myhbasecluster.azurehdinsight.net";
        const string HADOOPUSERNAME = "admin";
        const string HADOOPUSERPASSWORD = "MyPassWord#";
        const string HBASETABLENAME = "tweets_by_words";

        // The constructor
        public HBaseReader()
        {
            ClusterCredentials creds = new ClusterCredentials(
                            new Uri(CLUSTERNAME),
                            HADOOPUSERNAME,
                            HADOOPUSERPASSWORD);
            client = new HBaseClient(creds);
        }

        // Query Tweets sentiment data from the HBase table asynchronously 
        public async Task<IEnumerable<Tweet>> QueryTweetsByKeywordAsync(string keyword)
        {
            List<Tweet> list = new List<Tweet>();

            // Demonstrate Filtering the data from the past 6 hours the row key
            string timeIndex = (ulong.MaxValue -
                (ulong)DateTime.UtcNow.Subtract(new TimeSpan(6, 0, 0)).ToBinary()).ToString().PadLeft(20);
            string startRow = keyword + "_" + timeIndex;
            string endRow = keyword + "|";
            Scanner scanSettings = new Scanner
            {
                batch = 100000,
                startRow = Encoding.UTF8.GetBytes(startRow),
                endRow = Encoding.UTF8.GetBytes(endRow)
            };

            // Make async scan call
            ScannerInformation scannerInfo =
                await client.CreateScannerAsync(HBASETABLENAME, scanSettings);

            CellSet next;

            while ((next = await client.ScannerGetNextAsync(scannerInfo)) != null)
            {
                foreach (CellSet.Row row in next.rows)
                {
                    // find the cell with string pattern "d:coor" 
                    var coordinates =
                        row.values.Find(c => Encoding.UTF8.GetString(c.column) == "d:coor");

                    if (coordinates != null)
                    {
                        string[] lonlat = Encoding.UTF8.GetString(coordinates.data).Split(',');

                        var sentimentField =
                            row.values.Find(c => Encoding.UTF8.GetString(c.column) == "d:sentiment");
                        Int32 sentiment = 0;
                        if (sentimentField != null)
                        {
                            sentiment = Convert.ToInt32(Encoding.UTF8.GetString(sentimentField.data));
                        }

                        list.Add(new Tweet
                        {
                            Longtitude = Convert.ToDouble(lonlat[0]),
                            Latitude = Convert.ToDouble(lonlat[1]),
                            Sentiment = sentiment
                        });
                    }

                    if (coordinates != null)
                    {
                        string[] lonlat = Encoding.UTF8.GetString(coordinates.data).Split(',');
                    }
                }
            }

            return list;
        }
    }

    public class Tweet
    {
        public string IdStr { get; set; }
        public string Text { get; set; }
        public string Lang { get; set; }
        public double Longtitude { get; set; }
        public double Latitude { get; set; }
        public int Sentiment { get; set; }
    }
}