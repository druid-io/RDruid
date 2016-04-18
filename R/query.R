#
# Copyright 2013 Metamarkets Group Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Druid query types

#' Construct the URL for a druid endpoint.
#'
#' Defaults to http://localhost:8082/druid/v2/
#'
#' @param host hostname
#' @param port port
#'
#' @return The URL for the druid endpoint
#' @export
druid.url <- function(host = "localhost", port = 8082) {
    return (paste("http://", host, ":", port, "/druid/v2/", sep=""))
}

#' Converts JSON from Druid into a data frame
#'
#' Retrieves the JSON result from Druid, then formats it into a dataframe with
#' a timestamp column
#'
#' @param result Druid query result
#' @param resultFn function to transform results
#' @seealso \code{\link{query}}
#'
druid.resulttodf <- function(result, resultFn = identity, ...) {
  ts   <- plyr::laply(result, function(x) { x$timestamp })
  data <- plyr::ldply(result, function(x) { as.data.frame(resultFn(x$result)) })
  df <- cbind(timestamp = ts, data)

  # convert timestamp to POSIXct
  if(!is.null(df$timestamp)) {
      df$timestamp <- fromISO(df$timestamp)
  }
  return(df)
}

#' Convert Druid groupBy query result to a data frame
#'
#' Retrieves the JSON result from Druid, then formats it into a dataframe with
#' a timestamp column
#'
#' @param result The result from query
#' @seealso \code{\link{query}}
#'
druid.groupBytodf <- function(result) {
  # extract event timestamps
  ts   <- plyr::laply(result, function(x) { x$timestamp })

  # extract columns
  cols <- c()
  plyr::l_ply(result, function(x) { cols <<- union(cols, names(x$event)) })

  # initialize data frame with the right dimensions
  df <- data.frame(matrix(ncol=length(cols), nrow=length(result)), stringsAsFactors=F)
  names(df) <- cols

  # fill in columns
  for(c in cols) {
    df[, c] <- plyr::laply(result, function(x){ v <- x$event[c][[1]]; if(is.null(v)) NA else v})
  }

  df <- cbind(timestamp = ts, df)

  # convert timestamp to POSIXct
  if(!is.null(df$timestamp)) {
    df$timestamp <- fromISO(df$timestamp)
  }
  return(df)
}

#' Query Druid data sources
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @return a character vector with the list of data sources
#' @export
druid.query.dataSources <- function(url = druid.url()) {
  query(NULL, paste(url, "datasources", sep=""))
}

#' Query data source dimensions
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @param dataSource name of the data source to query
#' @return a character vector with the list of dimensions
#' @export
druid.query.dimensions <- function(url = druid.url(), dataSource, interval=NULL, ...) {
  query(NULL, paste(url, "datasources/", dataSource, sep=""), query = list(interval = toISO(interval)), ...)$dimensions
}

#' Query data source metrics
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @param dataSource name of the data source to query
#' @param interval interval to query metrics for
#' @return a character vector with the list of metrics
#' @export
druid.query.metrics <- function(url = druid.url(), dataSource, interval=NULL) {
  query(NULL, paste(url, "datasources/", dataSource, sep=""), query = list(interval = toISO(interval)))$metrics
}

#' Query segment metadata
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @param dataSource name of the data source to query
#' @param intervals time period to retrieve data for
#' @param verbose prints out the JSON query sent to Druid
#'
#' @export
druid.query.segmentMetadata <- function(url = druid.url(),
                                        dataSource,
                                        intervals,
                                        verbose=F,
                                        ...) {
  query.js <- json(list(dataSource = dataSource,
                        queryType = "segmentMetadata",
                        intervals = as.list(toISO(intervals))
                   ), pretty=verbose)
  if(verbose) cat(query.js)
  query(query.js, url, verbose, ...)
}

#' Query data source time boundaries
#'
#' Query a datasource to get the earliest and latest timestamp available
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @param dataSource name of the data source to query
#' @param intervals time period to retrieve data for as an interval or list of interval objects
#' @param verbose prints out the JSON query sent to druid
#' @return a vector of POSIXct date-time objects
#'
#' @seealso \code{\link{Interval-class}}
#'
#' @examples \dontrun{
#'
#' # query min and max time
#' t <- druid.query.timeBoundary(
#'   druid.url(host = "xx.xx.xx.xx"),
#'   dataSource = "mydata"
#' )
#' t["minTime"]
#' t["maxTime"]
#'
#' }
#'
#' @export
druid.query.timeBoundary <- function(
  url = druid.url(),
  dataSource,
  intervals = NULL,
  bound = NULL,
  verbose=F, ...
) {
  query.list <- list(queryType = "timeBoundary", dataSource = dataSource)
  if(!is.null(intervals)) {
    query.list$intervals <- as.list(toISO(intervals))
  }
  if(!is.null(bound)) {
    if (!(bound %in% c('maxTime', 'minTime')))
      stop("bound not recognized: ", bound)
    query.list$bound <- bound
  }
  query.js <- json(query.list, pretty=verbose)
  if(verbose) cat(query.js)
  result.l <- query(query.js, url, verbose, ...)
  if(length(result.l) > 0) {
    lapply(result.l[[1]]$result, fromISO)
  } else {
    NA
  }
}

#' Query time series data
#'
#' Queries druid for timeseries data and returns it as a data frame
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @param dataSource name of the data source to query
#' @param intervals time period to retrieve data for
#'        as an interval object or list of interval objects
#' @param aggregations list of metric aggregations to compute for this datasource
#' @param filter filter specifying the subset of the data to extract.
#' @param granularity time granularity at which to aggregate
#' @param postAggregations post-aggregations to perform on the aggregations
#' @param context query context
#' @param rawData if set, returns the result object as is, without converting to a data frame
#' @param ... additional parameters to pass to druid.resulttodf
#' @param verbose prints out the JSON query sent to druid
#'
#' @return Returns a data frame where each column represents a time series
#'
#' @examples \dontrun{
#'
#'    # Get the time series associated with the twitter hashtag #druid, by hour
#'    druid.query.timeseries(url = druid.url(host = "<hostname>"),
#'                          dataSource   = "twitter",
#'                          intervals    = interval(ymd("2012-07-01"), ymd("2012-07-15")),
#'                          aggregations = sum(metric("count")),
#'                          filter       = dimension("hashtag") == "druid",
#'                          granularity  = granularity("hour"))
#'
#'    # Average tweet length for a combination of hashtags in a given time zone
#'    druid.query.timeseries(url = druid.url("<hostname>"),
#'                          dataSource   = "twitter",
#'                          intervals    = interval(ymd("2012-07-01"), ymd("2012-08-30")),
#'                          aggregations = list(
#'                                            sum(metric("count")),
#'                                            sum(metric("length")
#'                                         ),
#'                          postAggregations = list(
#'                                            avg_length = field("length") / field("count")
#'                                         )
#'                          filter       =   dimension("hashtag") == "london2012"
#'                                         | dimension("hashtag") == "olympics",
#'                          granularity  = granularity("PT6H", timeZone="Europe/London"))
#'   }
#' @seealso \code{\link{druid.query.groupBy}} \code{\link{druid.query.topN}} \code{\link{granularity}}
#' @export
druid.query.timeseries <- function(url = druid.url(), dataSource, intervals, aggregations, filter = NULL,
                                  granularity = "all", postAggregations = NULL, context = NULL, rawData = FALSE, verbose = F, ...) {
    # check whether aggregations is a list or a single aggregation object
    if(is(aggregations, "druid.aggregator")) aggregations <- list(aggregations)

    query.js <- json(list(intervals = as.list(toISO(intervals)),
                          aggregations = renameagg(aggregations),
                          dataSource = dataSource,
                          filter = filter,
                          granularity = granularity,
                          postAggregations = renameagg(postAggregations),
                          queryType = "timeseries",
                          context = context), pretty=verbose)
    result.l = query(query.js, url, verbose, ...)

    if(rawData) {
      return(result.l)
    }
    else {
      return(druid.resulttodf(result.l, ...))
    }
}

#' groupBy query
#'
#' Sends a groupBy query to Druid and returns the results as a data frame
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @param dataSource name of the data source to query
#' @param intervals the time period to retrieve data for as an interval or list of interval objects
#' @param aggregations list of metric aggregations to compute for this datasource
#'   See druid.build.aggregation
#' @param filter The filter specifying the subset of the data to extract.
#'   See druid.build.filter
#' @param having The having clause identifying which rows should be returned.
#'   See druid.build.having
#' @param granularity time granularity at which to aggregate, can be "all", "day", "hour", "minute"
#' @param dimensions list of dimensions along which to group data by
#' @param postAggregations Further operations to perform after the data has
#'   been filtered and aggregated.
#' @param orderBy list of columns defining the output order
#' @param limit number of results to limit output based on the ordering defined in orderBy
#' @param context query context
#' @param rawData boolean indicating whether or not to return the JSON in a list before converting to a data frame
#' @param verbose prints out the JSON query sent to druid
#' @return Returns a dataframe where each column represents a time series
#' @seealso \code{\link{druid.query.timeseries}} \code{\link{druid.query.topN}}
#' @export
druid.query.groupBy <- function(url = druid.url(), dataSource, intervals, aggregations, filter = NULL,
                               granularity = "all", dimensions = NULL, postAggregations = NULL,
                               having = NULL, orderBy = NULL, limit = NULL,
                               context = NULL, rawData = FALSE, verbose = F, ...) {
  # check whether aggregations is a list or a single aggregation object
  if(is(aggregations, "druid.aggregator")) aggregations <- list(aggregations)

  # make sure dimensions is a list
  if(!is.list(dimensions)) dimensions <- as.list(dimensions)

  limitSpec <- NULL
  if(!is.null(limit) && !is.null(orderBy)) {
    orderBySpec <- function(x) { list(dimension = x, direction = "DESCENDING") }
    cols <- plyr::llply(orderBy, eval, list(desc=orderBySpec))
    limitSpec <- list(type="default", columns = cols, limit = as.numeric(limit))
  }

  query.js <- json(list(intervals = as.list(toISO(intervals)),
                        aggregations = renameagg(aggregations),
                        dataSource = dataSource,
                        filter = filter,
                        having = having,
                        granularity = granularity,
                        dimensions = dimensions,
                        postAggregations = renameagg(postAggregations),
                        limitSpec = limitSpec,
                        queryType = "groupBy",
                        context = context), pretty=verbose)
  if(verbose) cat(query.js)
  queryResult <- query(query.js, url, verbose, ...)
  result.l <- tryCatch(queryResult,
                      error = function(e) print(queryResult))

  if(rawData) {
    return(result.l)
  }
  else {
    return(druid.groupBytodf(result.l))
  }
}



#' Query to find the topN dimension values of a datasource
#'
#' For a particular datasource, find the top n dimension values for a given metric
#'
#' @param url URL to connect to druid, defaults to druid.url()
#' @param dataSource name of the data source to query
#' @param intervals the time period to retrieve data for as an interval or list of interval objects
#' @param aggregations list of metric aggregations to compute for this datasource
#' @param filter The filter specifying the subset of the data to extract.
#' @param granularity time granularity for finding topN values, can be "all", "day", "hour", "minute".
#' @param postAggregations Further operations to perform after the data has
#'   been filtered and aggregated.
#' @param n The number of dimensions to return
#' @param dimension name of the dimension over which to compute top N
#' @param metric name of the metric (aggregation) used to rank values in top N
#' @param rawData boolean indicating whether or not to return the JSON in a list before converting to a data frame
#' @param verbose prints out the JSON query sent to druid
#' @return Returns a dataframe with the largest values of the dimension,
#'   as well as the requested metrics
#' @seealso \code{\link{druid.query.timeseries}} \code{\link{druid.query.groupBy}}
#' @export
druid.query.topN <- function(url = druid.url(), dataSource, intervals, aggregations, filter = NULL,
                             granularity = "all", postAggregations = NULL,
                             n, dimension, metric, context = NULL, rawData = F, verbose = F, ...) {
  # check whether aggregations is a list or a single aggregation object
  if(is(aggregations, "druid.aggregator")) aggregations <- list(aggregations)

  query.js <- json(list(intervals = as.list(toISO(intervals)),
                                 aggregations = renameagg(aggregations),
                                 dataSource = dataSource,
                                 filter = filter,
                                 granularity = granularity,
                                 postAggregations = renameagg(postAggregations),
                                 context = context,
                                 queryType = "topN", dimension = dimension,
                                 metric = metric, threshold = n), pretty=verbose)
  if(verbose) {
    cat(query.js)
  }
  result.l <- query(query.js, url, verbose, ...)

  if(rawData) {
    return (result.l)
  }
  else {
    ret <- plyr::llply(result.l, function(x) {
      list(timestamp = x[[1]],
           result    = plyr::ldply(x[[2]], function(y) { as.data.frame(y) })
      )
    })
    if(granularity == "all" && length(ret) > 0) ret[[1]]$result
    else ret
  }
}
