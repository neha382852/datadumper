using Aerospike.Client;
using CsvHelper;
using DataDumper.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataDumper
{
    class Program
    {
        static void Main(string[] args)
        { 
            var aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
            string nameSpace = "AirEngine";
            string setName = "Neha";
            string filepath = @"C:\Users\nanand\Desktop\2018-08-charlottesville-twitter-trolls\data\tweets3.csv";
            StreamReader streamreader = new StreamReader(filepath);
            CsvReader csvread = new CsvReader(streamreader);
            IEnumerable<Tweet> record = csvread.GetRecords<Tweet>();
            int rowcount = 0;
            foreach (var iterator in record) 
            {
                if (rowcount == 2000)
                    break;
                var key = new Key(nameSpace, setName, iterator.tweet_id);
                rowcount++;
                aerospikeClient.Put(new WritePolicy(), key, new Bin[] { new Bin("author", iterator.author), new Bin("Content", iterator.content), new Bin("Region", iterator.region), new Bin("Language", iterator.language), new Bin("tweetDate", iterator.tweet_date), new Bin("Tweet_time", iterator.tweet_time), new Bin("Year", iterator.year), new Bin("Month", iterator.month), new Bin("Hour", iterator.hour), new Bin("Minute", iterator.minute), new Bin("following", iterator.following), new Bin("Follower", iterator.followers), new Bin("Post_url", iterator.post_url), new Bin("Post_Type", iterator.post_type), new Bin("Retweet", iterator.retweet), new Bin("tweet_Id", iterator.tweet_id), new Bin("Author_id", iterator.author_id), new Bin("accountcat", iterator.account_category), new Bin("newjune2018", iterator.new_june_2018) });
            }
        
            streamreader.Close();
        }

    }
 }

