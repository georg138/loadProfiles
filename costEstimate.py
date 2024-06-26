import numpy as np
import datetime
import json
from datetime import datetime, UTC, timedelta
from pprint import pprint
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions

from flask import Flask

app = Flask(__name__)


windowDuration = timedelta(minutes=5)
solarCostEPerKwh = 0.09
constantLoad = 5.0
baseLoad = 0.5

now = datetime.now(UTC).astimezone()
start = datetime(now.year, now.month, now.day, now.hour, tzinfo=now.tzinfo)
morning = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo) + timedelta(hours=6)
noon = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo) + timedelta(hours=14)
evening = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo) + timedelta(hours=18)


for time in (morning, noon, evening, morning + timedelta(days=1)):
    if time > now:
        targetTime = time
        break


p = {"_start": start,
     "_stop": start + timedelta(hours=36),
     "_window": windowDuration}

client = InfluxDBClient(
    url="http://192.168.2.105:8086",
    token="ubJqLqH0T3H0waB_BTeP9UFb80kQkUuPWtupcAoFEElllyhwffVnxNWofyaovPCvYdEpDaEg0aLA6ezsenyS1Q==",
    org="openhab"
)
query_api = client.query_api()

query = f'import "date" \
from(bucket: "telegraf") \
  |> range(start: date.truncate(t: _start, unit: 1h), stop: _stop) \
  |> filter(fn: (r) => (r["_measurement"] == "solcast" and r["_field"] == "pv_estimate" and r["type"] == "forecasts") or (r["_measurement"] == "tibber" and r["_field"] == "total")) \
  |> aggregateWindow(every: _window, fn: mean, timeSrc: "_start") \
  |> fill(usePrevious: true) \
  |> fill(value: 0.0) \
  |> group() \
  |> pivot(rowKey:["_time"], columnKey: ["_measurement"], valueColumn: "_value") \
  |> rename(columns: {{solcast: "solarPowerKw", tibber: "gridCostPerKwh"}})'
availableResult = query_api.query(query, params=p)

query = f'import "date" \
import "generate" \
generate.from(count: 2, fn: (n) => 1, start: time(v: 0), stop: date.add(d: 3h, to: time(v: 0))) \
  |> map(fn: (r) => ({{r with _value: {constantLoad}}})) \
  |> range(start: time(v: 0), stop: date.add(d: 3h, to: time(v: 0))) \
  |> aggregateWindow(every: _window, fn: mean, timeSrc: "_start") \
  |> fill(usePrevious: true) \
  |> group()'
# loadProfileResult = query_api.query(query)

dbTimes = {"4h":
           {"_start": datetime(2024, 6, 5, 20, 35, tzinfo=UTC),
            "_stop": datetime(2024, 6, 6, 3, 50, tzinfo=UTC)},
           "4ha":
           {"_start": datetime(2024, 6, 18, 6, 10, tzinfo=UTC),
            "_stop": datetime(2024, 6, 18, 10, 10, tzinfo=UTC)},
           "1h30":
           {"_start": datetime(2024, 6, 6, 9, 0, tzinfo=UTC),
            "_stop": datetime(2024, 6, 6, 10, 35, tzinfo=UTC)},
           "1h30a":
           {"_start": datetime(2024, 6, 17, 15, 32, tzinfo=UTC),
            "_stop": datetime(2024, 6, 17, 17, 3, tzinfo=UTC)},
           "2h40":
           {"_start": datetime(2024, 6, 6, 13, 5, tzinfo=UTC),
            "_stop": datetime(2024, 6, 6, 15, 50, tzinfo=UTC)},
           "2h40a":
           {"_start": datetime(2024, 6, 16, 17, 5, tzinfo=UTC),
            "_stop": datetime(2024, 6, 16, 19, 50, tzinfo=UTC)}}

loadProfileQuery = '''import "date"
  from(bucket: "openhab")
  |> range(start: date.sub(d: 3h, from: _start), stop: _stop)
  |> filter(fn: (r) => r["_measurement"] == "EG_Kueche_Geschirrspueler_Leistung")
  |> aggregateWindow(every: 10s, fn: last, createEmpty: true, timeSrc: "_start")
  |> fill(usePrevious: true)
  |> range(start: _start, stop: _stop)
  |> aggregateWindow(every: _window, fn: mean, createEmpty: true, timeSrc: "_start")
  |> map(fn: (r) => ({r with _value: r._value / 1000.0}))
  |> timeShift(duration: duration(v: -int(v: _start)))'''

query = f'import "date" \
import "generate" \
generate.from(count: 2, fn: (n) => 1, start: date.truncate(t: _start, unit: 1h), stop: _stop) \
  |> map(fn: (r) => ({{r with _value: {baseLoad}}})) \
  |> range(start: date.truncate(t: _start, unit: 1h), stop: _stop) \
  |> aggregateWindow(every: _window, fn: mean) \
  |> fill(usePrevious: true) \
  |> group() \
  |> rename(columns: {{_value: "basePower"}})'
baseProfileResult = query_api.query(query, params=p)

times = np.array([record.get_time() for record in availableResult[0].records])
solarPowerKw = np.array([record.values.get("solarPowerKw") for record in availableResult[0].records])
gridCostPerKwh = np.array([record.values.get("gridCostPerKwh") for record in availableResult[0].records])
basePower = np.array([record.values.get("basePower") for record in baseProfileResult[0].records])
availableSolarPowerKw = np.fmax(solarPowerKw - basePower, 0.0)


def window(start, stop, requiredPower):
    usedGridPowerKw = np.fmax(requiredPower - availableSolarPowerKw[start:stop], 0.0)
    gridWorkKwh = np.sum(usedGridPowerKw) * 5 / 60
    gridCostE = np.sum(gridCostPerKwh[start:stop] * usedGridPowerKw) * 5 / 60
    solarWorkKwh = np.sum(np.fmin(availableSolarPowerKw[start:stop], requiredPower)) * 5 / 60
    solarCostE = solarWorkKwh * solarCostEPerKwh
    return np.array([gridCostE, solarCostE, gridCostE + solarCostE, gridWorkKwh, solarWorkKwh, gridWorkKwh + solarWorkKwh])

    # costEntry = {"time": times[start], "measurement": "cost", "fields": {
    #    "total": solarCostE+gridCostE, "grid": gridCostE, "solar": solarCostE}}
    # workEntry = {"time": times[start], "measurement": "work", "fields": {
    #    "total": solarWorkKwh + gridWorkKwh, "grid": gridWorkKwh, "solar": solarWorkKwh}}
    # return [costEntry, workEntry]


def minCostForProgram(timeParams, timeLimit):
    loadProfileResult = query_api.query(loadProfileQuery, params=p | timeParams)
    requiredPower = np.array([record.get_value() for record in loadProfileResult[0].records])
    requiredTime = loadProfileResult[0].records[-1].get_time() - loadProfileResult[0].records[0].get_time()

    costs = np.array([window(i, i + len(requiredPower), requiredPower)
                     for i in range(0, len(gridCostPerKwh) - len(requiredPower) + 1)])

    startIndex = round((now - start) / windowDuration)

    endTime = timeLimit - requiredTime
    endIndex = round((endTime - start) / windowDuration)

    # costsAtTime = np.hstack((times[:len(costs)].reshape(-1, 1), np.vstack(costs)), dtype=object).tolist()

    # print(costs)
    minIndex = costs[startIndex:endIndex, 2].argmin() + startIndex

    minCost = costs[minIndex]

    # print(costs[minIndex])
    # print(times[minIndex].astimezone())
    return {
        "time": times[minIndex].astimezone(),
        "gridCost": f"{minCost[0]} €",
        "solarCost": f"{minCost[1]} €",
        "totalCost": f"{minCost[2]} €",
        "gridWork": f"{minCost[3]} kWh",
        "solarWork": f"{minCost[4]} kWh",
        "totalWork": f"{minCost[5]} kWh"}
    # print(times[endIndex])


@app.route("/")
def hello_world():
    # return "x"
    result = {}
    for timeLimit in [targetTime]:
        result[str(timeLimit)] = {}
        for program, timeParams in dbTimes.items():
            # print(program)
            result[str(timeLimit)][program] = minCostForProgram(timeParams, timeLimit)

    return result


# print(availableSolarPowerKw)
# pprint([f"{x[0].hour}: {x[1]} {x[2]} {x[3]}" for x in costs[::12]])

# delete_api = client.delete_api()

# start = times[0] - datetime.timedelta(hours=1)
# stop = times[-1]
# delete_api.delete(start, stop, '_measurement="cost"', bucket='loadManagement', org='openhab')
# delete_api.delete(start, stop, '_measurement="work"', bucket='loadManagement', org='openhab')


# write_api = client.write_api(write_options=SYNCHRONOUS)

# write_api.write("loadManagement", "openhab", (Point("cost").time(x[0]).field(
#    "gridCost", x[1]).field("solarCost", x[2]).field("totalCost", x[3]) for x in costs))


# write_api.write("loadManagement", "openhab", costs)

client.close()


if __name__ == "__main__":
    print(json.dumps(hello_world(), indent=2, default=str, ensure_ascii=False))
