using ETL.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Transactions;

namespace ETL.Utils;

public class DbManager(SqlConnection connection)
{
    public void CreateDatabase()
    {
        ExecuteScript("Scripts\\create_db.sql");
    }

    public void CreateTable()
    {
        ExecuteScript("Scripts\\create_table.sql");
    }

    public DataTable GetDublicates(SqlTransaction transaction)
    {
        var path = "Scripts\\get_dublicates.sql";
        var script = File.ReadAllText(path);

        using var command = new SqlCommand(script, connection, transaction);
        using var reader = command.ExecuteReader();

        var table = new DataTable();
        table.Load(reader);

        return table;
    }

    public void DeleteDublicates(SqlTransaction transaction)
    {
        ExecuteScript("Scripts\\delete_dublicates.sql", transaction);
    }

    public void Upload(IEnumerable<RideRecord> records, SqlTransaction transaction)
    {
        using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
        {
            DestinationTableName = "ride_records",
            BatchSize = records.Count()
        };

        var table = ToDataTable(records);

        bulkCopy.WriteToServer(table);
    }

    private void ExecuteScript(string path, SqlTransaction? transaction = null)
    {
        var script = File.ReadAllText(path);

        using var command = new SqlCommand(script, connection, transaction);
        command.ExecuteNonQuery();
    }

    private static DataTable ToDataTable(IEnumerable<RideRecord> batch)
    {
        var table = new DataTable();

        table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
        table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
        table.Columns.Add("passenger_count", typeof(short));
        table.Columns.Add("trip_distance", typeof(double));
        table.Columns.Add("store_and_fwd_flag", typeof(string));
        table.Columns.Add("pu_location_id", typeof(int));
        table.Columns.Add("do_location_id", typeof(int));
        table.Columns.Add("fare_amount", typeof(decimal));
        table.Columns.Add("tip_amount", typeof(decimal));

        foreach (var trip in batch)
        {
            // Convert flag.
            if (trip.StoreAndFwdFlag == "Y")
            {
                trip.StoreAndFwdFlag = "Yes";
            }
            else if (trip.StoreAndFwdFlag == "N")
            {
                trip.StoreAndFwdFlag = "No";
            }

            // Convert timezone.
            var estZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

            table.Rows.Add(
                TimeZoneInfo.ConvertTimeToUtc(trip.PickupDatetime, estZone),
                TimeZoneInfo.ConvertTimeToUtc(trip.DropoffDatetime, estZone),
                trip.PassengerCount ?? 0,
                trip.TripDistance ?? 0,
                trip.StoreAndFwdFlag.Trim(),
                trip.PULocationID ?? (object)DBNull.Value,
                trip.DOLocationID ?? (object)DBNull.Value,
                trip.FareAmount ?? 0m,
                trip.TipAmount ?? 0m
            );
        }

        return table;
    }
}
