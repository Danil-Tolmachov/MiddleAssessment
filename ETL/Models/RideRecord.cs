using CsvHelper.Configuration.Attributes;

namespace ETL.Models;

public class RideRecord
{
    [Name("tpep_pickup_datetime")]
    public DateTime PickupDatetime { get; set; }
    [Name("tpep_dropoff_datetime")]
    public DateTime DropoffDatetime { get; set; }

    [Name("passenger_count")]
    public short? PassengerCount { get; set; }
    [Name("trip_distance")]
    public double? TripDistance { get; set; }

    [Name("store_and_fwd_flag")]
    public string StoreAndFwdFlag { get; set; } = string.Empty;

    [Name("PULocationID")]
    public int? PULocationID { get; set; }
    [Name("DOLocationID")]
    public int? DOLocationID { get; set; }

    [Name("fare_amount")]
    public decimal? FareAmount { get; set; }
    [Name("tip_amount")]
    public decimal? TipAmount { get; set; }

    public string DuplicateKey =>
        $"{PickupDatetime:O}|{DropoffDatetime:O}|{PassengerCount}";
}
