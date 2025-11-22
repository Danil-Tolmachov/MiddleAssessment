using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Configuration;
using System.Globalization;
using System.Reflection;
using ETL.Utils;
using Microsoft.Data.SqlClient;



var config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                       .AddJsonFile("config.json", optional: false)
                                       .Build();

var batchSize = int.Parse(config.GetSection("BatchSize").Value ?? "200"); // Default - 200

var connectionString = config.GetSection("ConnectionString").Value
    ?? throw new ConfigurationException("Connection string value is not set in configuration.");
var exportDublicatesPath = config.GetSection("dublicatesExportFilePath").Value
    ?? throw new ConfigurationException("Dublicates export path string value is not set in configuration.");


// User entrypoint.
Console.WriteLine("Press any key to start.");
Console.ReadKey();

Console.Clear();
Console.WriteLine("Processing csv file...");

var assembly = Assembly.GetExecutingAssembly();
using Stream csvFileStream = assembly.GetManifestResourceStream("ETL.sample-cab-data.csv")
    ?? throw new InvalidOperationException("Failed to load csv file.");

// Read and Insert by batch.
using var reader = new StreamReader(csvFileStream);
using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
                                       {
                                           TrimOptions = TrimOptions.Trim,
                                           IgnoreBlankLines = true,
                                           HasHeaderRecord = true
                                       }))
{
    csv.Read();
    csv.ReadHeader();

    using var connection = new SqlConnection(connectionString);
    connection.Open();


    var dbm = new DbManager(connection);


    dbm.CreateDatabase();
    dbm.CreateTable();

    using var transaction = connection.BeginTransaction();

    try
    {
        // Insert records.
        while (true)
        {
            // Get batch.
            var batch = CsvFileHelper.ReadBatch(csv, batchSize);
        
            if (!batch.Any())
            {
                break;
            }
        
            // Upload batch to db.
            dbm.Upload(batch, transaction);
        }

        // Commit
        //transaction.Commit();

        // Remove dublicates.
        var dublicates = dbm.GetDublicates(transaction);
        CsvFileHelper.ExportRecords(dublicates, exportDublicatesPath);
        dbm.DeleteDublicates(transaction);

        // Commit
        transaction.Commit();
    }
    catch (Exception ex) 
    {
        transaction.Rollback();
        throw new InvalidOperationException($"Failed to process csv.", ex);
    }
    finally
    {
        connection.Close();
    }

}