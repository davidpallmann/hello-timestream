using Amazon.TimestreamWrite.Model;
using Microsoft.Extensions.Configuration;

namespace HelloTimestream
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            IConfiguration config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var databaseName = config["timestream:database"];
            var tableName = config["timestream:table"];

            TimestreamHelper timestreamHelper = new TimestreamHelper(databaseName, tableName);

            if (args.Length == 1 && args[0] == "list")
            {
                // Query all bird sighting records

                var results = await timestreamHelper.QueryRecordsAsync($"select * from \"{databaseName}\".{tableName}");
                foreach(string line in results)
                {
                    Console.WriteLine(line);
                }
                Environment.Exit(0);
            }

            if (args.Length == 1 && args[0] == "recent")
            {
                // Query most recent bird sighting records

                Console.WriteLine("Most recent records:");
                var results = await timestreamHelper.QueryRecordsAsync($"select * from \"{databaseName}\".{tableName} order by time DESC limit 5");
                foreach (string line in results)
                {
                    Console.WriteLine(line);
                }
                Environment.Exit(0);
            }

            if (args.Length == 2 && args[0] == "sightings")
            {
                // Query sightings for a specific species

                var species = args[1];
                Console.WriteLine($"Sightings for {species}:");
                var results = await timestreamHelper.QueryRecordsAsync($"select * from \"{databaseName}\".{tableName} where species='{species}' order by time ASC");
                foreach (string line in results)
                {
                    Console.WriteLine(line);
                }
                Environment.Exit(0);
            }

            if (args.Length < 5)
            {
                Console.WriteLine("To write records:       dotnet run -- [city] [region] [country] [species] [count]");
                Console.WriteLine("                        dotnet run -- Seattle WA USA \"Northern Spotted Owl\" 3");
                Console.WriteLine("                        dotnet run -- Olympia WA USA \"California Condor\" 1");
                Console.WriteLine("To list records:        dotnet run -- list");
                Console.WriteLine("To list recent records: dotnet run -- recent");
                Console.WriteLine("To list sightings:      dotnet run -- sightings [species]");
                Console.WriteLine("                        dotnet run -- sightings \"California Condor\"");
                Environment.Exit(0);
            }

            // Write bird sighting record

            try
            {
                var city = args[0];
                var region = args[1];
                var country = args[2];
                var species = args[3];
                var count = args[4];

                DateTimeOffset now = DateTimeOffset.UtcNow;
                string currentTimeString = (now.ToUnixTimeMilliseconds()).ToString();

                List<Dimension> dimensions = new List<Dimension> {
                    new Dimension { Name = "city", Value = city },
                    new Dimension { Name = "region", Value = region },
                    new Dimension { Name = "country", Value = country },
                    new Dimension { Name = "species", Value = species }
                };

                var birdSightingRecord = new Record
                {
                    Dimensions = dimensions,
                    MeasureName = "count",
                    MeasureValue = count,
                    MeasureValueType = Amazon.TimestreamWrite.MeasureValueType.BIGINT,
                    Time = currentTimeString
                };

                Console.WriteLine("Writing record");

                var response = await timestreamHelper.WriteRecordsAsync(birdSightingRecord);
                Console.WriteLine($"Write records status code: {response.HttpStatusCode.ToString()}");
                if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
                {
                    Console.WriteLine("Error writing records - bad HTTP response");
                }
            }
            catch (RejectedRecordsException ex)
            {
                Console.WriteLine("Record rejected: " + ex.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error writing records:" + ex.ToString());
            }
        }

    }
}