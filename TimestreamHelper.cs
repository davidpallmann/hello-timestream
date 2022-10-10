#pragma warning disable CS1998

using Amazon;
using Amazon.TimestreamQuery;
using Amazon.TimestreamQuery.Model;
using Amazon.TimestreamWrite;
using Amazon.TimestreamWrite.Model;
using System.Text.Json;

namespace HelloTimestream
{
    public class TimestreamHelper
    {
        public string DatabaseName = null!;
        public string TableName = null!;
        public const long HT_TTL_HOURS = 24;
        public const long CT_TTL_DAYS = 7;

        public TimestreamHelper(string database, string table)
        {
            DatabaseName = database;
            TableName = table;
        }

        #region Write

        /// <summary>
        /// Write records to timestream table.
        /// </summary>
        /// <param name="records">List of records to write</param>
        /// <returns><WriteRecordResponse/returns>
        public async Task<WriteRecordsResponse> WriteRecordsAsync(params Record[] records)
        {
            var writeClientConfig = new AmazonTimestreamWriteConfig
            {
                Timeout = TimeSpan.FromSeconds(20),
                MaxErrorRetry = 10
            };

            var writeClient = new AmazonTimestreamWriteClient(writeClientConfig);

            var writeRecordsRequest = new WriteRecordsRequest
            {
                DatabaseName = DatabaseName,
                TableName = TableName,
                Records = new List<Record>(records)
            };

            WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);

            return response;
        }

        #endregion

        #region Query

        /// <summary>
        /// Query timestream table
        /// Created based on https://docs.aws.amazon.com/timestream/latest/developerguide/code-samples.run-query.html
        /// </summary>
        /// <returns>List of strings</returns>
        public async Task<List<string>> QueryRecordsAsync(string query)
        {
            var results = new List<string>();

            var queryClientConfig = new AmazonTimestreamQueryConfig
            {
                Timeout = TimeSpan.FromSeconds(20),
                MaxErrorRetry = 10
            };

            var queryClient = new AmazonTimestreamQueryClient(queryClientConfig);


            var queryRequest = new QueryRequest()
            {
                QueryString = query
            };

            var queryResponse = await queryClient.QueryAsync(queryRequest);

            while (true)
            {
                results.AddRange(await ParseQueryResult(queryResponse));
                if (queryResponse.NextToken == null) break;
                queryRequest.NextToken = queryResponse.NextToken;
                queryResponse = await queryClient.QueryAsync(queryRequest);
            }

            return results;
        }

        private static async Task<List<string>> ParseQueryResult(QueryResponse response)
        {
            var results = new List<string>();

            List<ColumnInfo> columnInfo = response.ColumnInfo;
            var options = new JsonSerializerOptions
            { 
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            List<String> columnInfoStrings = columnInfo.ConvertAll(x => JsonSerializer.Serialize(x, options));
            List<Row> rows = response.Rows;

            QueryStatus queryStatus = response.QueryStatus;

            foreach (Row row in rows)
            {
                results.Add(ParseRow(columnInfo, row));
            }

            return results;
        }

        private static string ParseRow(List<ColumnInfo> columnInfo, Row row)
        {
            List<Datum> data = row.Data;
            List<string> rowOutput = new List<string>();
            for (int j = 0; j < data.Count; j++)
            {
                ColumnInfo info = columnInfo[j];
                Datum datum = data[j];
                rowOutput.Add(ParseDatum(info, datum));
            }
            return $"{{{string.Join(",", rowOutput)}}}";
        }

        private static string ParseDatum(ColumnInfo info, Datum datum)
        {
            if (datum.NullValue)
            {
                return $"{info.Name}=NULL";
            }

            Amazon.TimestreamQuery.Model.Type columnType = info.Type;
            if (columnType.TimeSeriesMeasureValueColumnInfo != null)
            {
                return ParseTimeSeries(info, datum);
            }
            else if (columnType.ArrayColumnInfo != null)
            {
                List<Datum> arrayValues = datum.ArrayValue;
                return $"{info.Name}={ParseArray(info.Type.ArrayColumnInfo, arrayValues)}";
            }
            else if (columnType.RowColumnInfo != null && columnType.RowColumnInfo.Count > 0)
            {
                List<ColumnInfo> rowColumnInfo = info.Type.RowColumnInfo;
                Row rowValue = datum.RowValue;
                return ParseRow(rowColumnInfo, rowValue);
            }
            else
            {
                return ParseScalarType(info, datum);
            }
        }


        private static string ParseTimeSeries(ColumnInfo info, Datum datum)
        {
            var timeseriesString = datum.TimeSeriesValue
                .Select(value => $"{{time={value.Time}, value={ParseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, value.Value)}}}")
                .Aggregate((current, next) => current + "," + next);

            return $"[{timeseriesString}]";
        }

        private static string ParseScalarType(ColumnInfo info, Datum datum)
        {
            return ParseColumnName(info) + datum.ScalarValue;
        }

        private static string ParseColumnName(ColumnInfo info)
        {
            return info.Name == null ? "" : (info.Name + "=");
        }

        private static string ParseArray(ColumnInfo arrayColumnInfo, List<Datum> arrayValues)
        {
            return $"[{arrayValues.Select(value => ParseDatum(arrayColumnInfo, value)).Aggregate((current, next) => current + "," + next)}]";
        }

        #endregion
    }
}