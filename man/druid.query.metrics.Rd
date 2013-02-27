\name{druid.query.metrics}
\alias{druid.query.metrics}
\title{Query data source metrics}
\usage{
  druid.query.metrics(url = druid.url(), dataSource)
}
\arguments{
  \item{url}{URL to connect to druid, defaults to
  druid.url()}

  \item{dataSource}{name of the data source to query}
}
\value{
  a character vector with the list of metrics
}
\description{
  Query data source metrics
}

