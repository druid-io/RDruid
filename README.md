RDruid
======

Druid connector for R

## Installation

To install the latest version of RDruid, it's easiest to use the `devtools` package:

```r
# install.packages("devtools")
library(devtools)
install_github("RDruid", "metamx")
```

## Examples

```r
# Number of edits and average number of characters added / deleted for a given wikipedia page
druid.query.timeseries(
  url = druid.url("<hostname>", port=8080),
  dataSource   = "wikipedia",
  intervals    = interval(
                    fromISO("2013-02-24T00:00:00-08:00"),
                    fromISO("2013-02-28T00:00:00-08:00")
                 ),
  aggregations = list(
   sum(metric("added")),
   sum(metric("deleted")),
   edits = sum(metric("count")) # alias sum("count") as "edits"
  ),
  postAggregations = list(
   average_added = field("added") / field("edits"),
   average_deleted = -1 * field("deleted") / field("edits")
  ),
  filter       =   dimension("namespace") == "article"
                 & dimension("page") == "85th_Academy_Awards",
  granularity  = granularity("PT1H", timeZone="America/Los_Angeles")
)
```

```r
# Group wikipedia edits and delta by day, language, and user
druid.query.groupBy(
  url = druid.url("<hostname>", port=8080),
  dataSource   = "wikipedia",
  intervals    = interval(
    fromISO("2013-02-24T00:00:00-08:00"),
    fromISO("2013-02-28T00:00:00-08:00")
  ),
  aggregations = list(
    change = sum(metric("delta")),
    edits  = sum(metric("count"))
  ),
  postAggregations = list(
    average_change = field("change") / field("edits")
  ),
  filter       =   dimension("namespace") == "article"
                 & dimension("page") == "85th_Academy_Awards",
  granularity  = granularity("P1D", timeZone="America/Los_Angeles"),
  dimensions   = c("language", "user")
)
```

```r
library(RDruid)
library(lubridate)

# Average tweet length for a combination of hashtags in a given time zone
druid.query.timeseries(
  url = druid.url("<hostname>"),
  dataSource   = "twitter",
  intervals    = interval(ymd("2012-07-01"), ymd("2012-08-30")),
  aggregations = list(
                    sum(metric("count")),
                    sum(metric("tweet_length"))
                 ),
  postAggregations = list(
                    avg_length = field("tweet_length") / field("count")
                 ),
  filter       =   dimension("hashtag") == "london2012"
                 | dimension("hashtag") == "olympics",
  granularity  = granularity("PT6H", timeZone="Europe/London")
)
```
