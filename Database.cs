using InfluxDB.Client;
using InfluxDB.Client.Linq;
using InfluxDB.Client.Core.Flux.Domain;
using InfluxDB.Client.Core;

public class Database
{
    private string _gridCostQuery = """
import "date"
from(bucket: "telegraf")
  |> range(start: date.truncate(t: _start, unit: 1h), stop: _stop)
  |> filter(fn: (r) => (r["_measurement"] == "solcast" and r["_field"] == "pv_estimate" and r["type"] == "forecasts") or (r["_measurement"] == "tibber" and r["_field"] == "total"))
  |> aggregateWindow(every: _window, fn: mean, timeSrc: "_start")
  |> fill(usePrevious: true)
  |> fill(value: 0.0)
  |> group()
  |> pivot(rowKey:["_time"], columnKey: ["_measurement"], valueColumn: "_value")
  |> rename(columns: {{solcast: "solarPowerKw", tibber: "gridCostPerKwh"}})
""";

    private readonly InfluxDBClient _client = new("http://192.168.2.105:8086",
        "ubJqLqH0T3H0waB_BTeP9UFb80kQkUuPWtupcAoFEElllyhwffVnxNWofyaovPCvYdEpDaEg0aLA6ezsenyS1Q==");


    public async Task<IEnumerable<object>> GetGridCostAsync()
    {
        // var res = await Query(_gridCostQuery);
        // return res[0].Records.Select(x => x.GetValueByKey("gridCostPerKwh"));
       var api= _client.GetQueryApiSync();
       var now = DateTime.Now;
      var data= InfluxDBQueryable<Sensor>.Queryable("openhab", "openhab", api)
       .Where(x=>x.Timestamp> now)
       .Where(x=>x.Timestamp< now + TimeSpan.FromDays(1)).ToList();

       return data;
    }

    private Task<List<FluxTable>> Query(string query) 
        => _client.GetQueryApi().QueryAsync(query, "openhab");
}

class Sensor
{
    [Column("_measurement", IsTag = true)] 
    public string Measurement { get; set; }

    [Column("_field", IsTag = true)]
    public string Field { get; set; }

    [Column("type", IsTag = true)]
    public string Type { get; set; }

    [Column("_value")]
    public float Value { get; set; }

    [Column(IsTimestamp = true)] 
    public DateTime Timestamp { get; set; }
}