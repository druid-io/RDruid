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
# Average tweet length for a combination of hashtags in a given time zone
druid.query.timeseries(url = druid.url("<hostname>"),
                       dataSource   = "twitter",
                       intervals    = interval(ymd("2012-07-01"), ymd("2012-08-30")),
                       aggregations = list(
                                         sum(metric("count")),
                                         sum(metric("length")
                                      ),
                       postAggregations = list(
                                         avg_length = field("length") / field("count")
                                      )
                       filter       =   dimension("hashtag") == "london2012"
                                      | dimension("hashtag") == "olympics",
                       granularity  = granularity("PT6H", timeZone="Europe/London"))
```
