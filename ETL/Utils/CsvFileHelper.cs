using CsvHelper;
using ETL.Models;
using System.Data;
using System.Globalization;

namespace ETL.Utils;

public static class CsvFileHelper
{
    public static void ExportRecords(DataTable table, string filePath)
    {
        using var fileStream = File.Open(filePath, FileMode.Append);

        using var writer = new StreamWriter(fileStream);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

        // Write headers.
        foreach (DataColumn col in table.Columns)
        {
            csv.WriteField(col.ColumnName);
        }
        csv.NextRecord();

        // Write rows.
        foreach (DataRow row in table.Rows)
        {
            foreach (var item in row.ItemArray)
            {
                csv.WriteField(item);
            }
            csv.NextRecord();
        }
    }

    public static IEnumerable<RideRecord> ReadBatch(CsvReader reader, int batchSize)
    {
        var batch = new List<RideRecord>(batchSize);

        while (reader.Read())
        {
            var record = reader.GetRecord<RideRecord>();
            batch.Add(record);

            if (batch.Count >= batchSize)
            {
                break;
            }
        }

        return batch;
    }
}
