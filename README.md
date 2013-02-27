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

## Example

```r
library(RDruid)
library(lubridate)

# Average tweet length for a combination of hashtags in a given time zone
druid.query.timeseries(url = druid.url("<hostname>"),
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
                       granularity  = granularity("PT6H", timeZone="Europe/London"))
```
