\name{javascript}
\alias{javascript}
\title{Constructs a JavaScript aggregator for the given Druid columns}
\usage{
  javascript(fieldNames, fnAggregate, fnCombine, fnReset,
    name = "javascript")
}
\arguments{
  \item{fieldNames}{list of Druid columns fnAggregate
  depends on}

  \item{fnAggregte}{JavaScript function updating the
  current aggregate value}

  \item{fnCombine}{JavaScript function combining aggregate
  values}

  \item{fnReset}{JavaScript function returning the initial
  aggregate value}
}
\description{
  Constructs a JavaScript aggregator for the given Druid
  columns
}

