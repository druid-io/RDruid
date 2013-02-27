\name{druid.groupBytodf}
\alias{druid.groupBytodf}
\title{Convert Druid groupBy query result to a data frame}
\usage{
  druid.groupBytodf(result)
}
\arguments{
  \item{result}{The result from query}
}
\description{
  Retrieves the JSON result from Druid, then formats it
  into a dataframe with a timestamp column
}
\seealso{
  \code{\link{query}}
}

