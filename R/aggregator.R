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


# Druid Aggregators

setClass("druid.aggregator", representation="list", S3methods=TRUE)
druid.aggregator <- function(...) new("druid.aggregator", ...)

#' Construct an arbitrary Druid aggregator using the given parameters
#' 
#' @param type aggregator type
#' @param ... aggregator arguments
#' @export
druid.build.aggregation <- function(type, ...) {
  structure(list(type = type, ...), class="druid.aggregator")
}

druid.agg.count <- function(name) {
  druid.build.aggregation(type="count", name = name)
}

druid.agg.longSum <- function(name, fieldName = name) {
  druid.build.aggregation(type="longSum", name = name, fieldName = fieldName)
}

druid.agg.doubleSum <- function(name, fieldName = name) {
  druid.build.aggregation(type="doubleSum", name = name, fieldName = fieldName)
}

druid.agg.min <- function(name, fieldName = name, ...) {
  druid.build.aggregation(type="min", name = name, fieldName = fieldName)
}

druid.agg.max <- function(name, fieldName = name, ...) {
  druid.build.aggregation(type="max", name = name, fieldName = fieldName)
}

#' Constructs a max aggregator over the given Druid metric
#' @param x metric
#' @param ... optional arguments to construct aggregator
#' @method max druid.metric
#' @export
max.druid.metric <- function(x, ...) {
  druid.agg.max(as.character(x), ...)
}

#' Constructs a min aggregator over the given Druid metric
#' 
#' @method min druid.metric
#' @param x metric
#' @param ... optional arguments to construct aggregator
#' @export
min.druid.metric <- function(x, ...) {
  druid.agg.min(as.character(x), ...)
}

#' Constructs a doubleSum aggregator over the given Druid metric
#' 
#' @param x metric
#' @param ... optional arguments to construct aggregator
#' @method sum druid.metric
#' @export
sum.druid.metric <- function(x, ...) {
  druid.agg.doubleSum(as.character(x))
}

#' Constructs a count aggregator
#' 
#' @param name aggregator name, defaults to 'count'
#' @export
druid.count <- function(name = "count") {
  druid.agg.count(name)
}

#' Constructs a longSum aggregator over the given Druid metric
#' 
#' @param x metric
#' @param ... optional arguments to construct aggregator
#' @export
longSum <- function(x, ...) {
  UseMethod("longSum", x)
}

#' Constructs a longSum aggregator over the given Druid metric
#' 
#' @param x metric
#' @param ... optional arguments to construct aggregator
#' @method longSum druid.metric
#' @export
longSum.druid.metric <- function(x, ...) {
  druid.agg.longSum(as.character(x), ...)
}

#' @method "&" druid.aggregator
#' @export
`&.druid.aggregator` <- function(a, b) {
  if(is.list(a) && !is(a, "druid.aggregator")) {
    c(a, list(b))
  }
  else if(is.list(b) && !is(b, "druid.aggregator")) {
    c(list(a), b)
  }
  else {
    list(a, b)
  }
}

#' @method print druid.aggregator
#' @export
print.druid.aggregator <- function(x, ...) {
  cat("druid aggregator: ", toString(x), "\n", sep="")
}

#' @method toString druid.aggregator
#' @export
toString.druid.aggregator <- function(x, ...) {
  paste(x$type, "(", x$name, ")", sep="")
}

#' Rename aggregators based on their list name
#' 
#' @param agglist list of aggregators
renameagg <- function(agglist) {
  newname <- names(agglist)
  if(is.null(newname)) return(agglist)
  
  for(i in 1:length(agglist)) {
    if(newname[i] != "") agglist[[i]]$name <- newname[i]
    #if(is(agglist[[i]], "druid.aggregator") && newname[i] != "") agglist[[i]]$fieldName <- newname[i]
  }
  names(agglist) <- NULL
  agglist
}
