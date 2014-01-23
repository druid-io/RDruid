\name{druid.query.groupBy}
\alias{druid.query.groupBy}
\title{groupBy query}
\usage{
  druid.query.groupBy(url = druid.url(), dataSource,
    intervals, aggregations, filter = NULL,
    granularity = "all", dimensions = NULL,
    postAggregations = NULL, having = NULL, orderBy = NULL,
    limit = NULL, context = NULL, rawData = FALSE,
    verbose = F, ...)
}
\arguments{
  \item{url}{URL to connect to druid, defaults to
  druid.url()}

  \item{dataSource}{name of the data source to query}

  \item{intervals}{the time period to retrieve data for as
  an interval or list of interval objects}

  \item{aggregations}{list of metric aggregations to
  compute for this datasource See druid.build.aggregation}

  \item{filter}{The filter specifying the subset of the
  data to extract.  See druid.build.filter}

  \item{having}{The having clause identifying which rows
  should be returned.  See druid.build.having}

  \item{granularity}{time granularity at which to
  aggregate, can be "all", "day", "hour", "minute"}

  \item{dimensions}{list of dimensions along which to group
  data by}

  \item{postAggregations}{Further operations to perform
  after the data has been filtered and aggregated.}

  \item{orderBy}{list of columns defining the output order}

  \item{limit}{number of results to limit output based on
  the ordering defined in orderBy}

  \item{context}{query context}

  \item{rawData}{boolean indicating whether or not to
  return the JSON in a list before converting to a data
  frame}

  \item{verbose}{prints out the JSON query sent to druid}
}
\value{
  Returns a dataframe where each column represents a time
  series
}
\description{
  Sends a groupBy query to Druid and returns the results as
  a data frame
}
\seealso{
  \code{\link{druid.query.timeseries}}
  \code{\link{druid.query.topN}}
}

