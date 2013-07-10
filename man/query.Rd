\name{query}
\alias{query}
\title{Send a JSON query to Druid}
\usage{
  query(jsonstr, url, verbose = F, benchmark = F, ...)
}
\arguments{
  \item{jsonstr}{JSON string containing details of the
  query.}

  \item{url}{the endpoint for where this query should be
  sent. Use druid.url() to construct the URL.}
}
\description{
  Takes a JSON string and uses RCurl to send a query to the
  endpoint specified by url.  Typically this function is
  not called on its own, instead the end user should use
  the query family of functions
}
\seealso{
  \code{\link{druid.query.timeseries}}
}
\keyword{database,}
\keyword{druid,}
\keyword{query}

