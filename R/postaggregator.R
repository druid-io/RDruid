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


# Druid Post-Aggregators

setClass("druid.postaggregator", representation="list", S3methods=TRUE)
druid.postaggregator <- function(...) new("druid.postaggregator", ...)

#' Creates a Druid post-aggregator
#' 
#' @param type post-aggregator type
#' @param ... additional post-aggregator parameters
#' @return a Druid post-aggregator object
#' @export
druid.build.postaggregator <- function(type, ...) {
  structure(list(type = type, ...), class="druid.postaggregator")
}

#' @method toString druid.postaggregator
#' @export
toString.druid.postaggregator <- function(x, ...) {
  switch(x$type,
         constant = x$value,
         arithmetic = paste("(", toString(x$fields[[1]]), x$fn, toString(x$fields[[2]]), ")"),
         fieldAccess = x$name
  )
}

#' @method print druid.postaggregator
#' @export
print.druid.postaggregator <- function(x, ...) {
  cat("druid postAggregator: ", toString(x), "\n", sep="")
}

#' Creates an arithmetic post-aggregator
#' 
#' @param fn operator (e.g. +, -, *, /)
#' @param a left hand side post-aggregator
#' @param b right hand side post-aggregator
#' @param name alias for this post-aggregator
druid.postagg.arithmetic <- function(fn, a, b, name = NULL) {
  if(is.null(name)) {
    name <- paste("(", a$name, fn, b$name, ")", sep="")
  }
  
  druid.build.postaggregator(type="arithmetic", fn=fn, name=name, fields = list(a, b))
}

#' Creates a constant post-aggregator
#' 
#' @param x numeric constant
druid.postagg.constant <- function(x) {
  druid.build.postaggregator(type="constant", name=as.character(x), value=as.numeric(x))
}

#' Creates a fieldAccess post-aggregator
#' 
#' @param name alias for this post-aggregator
#' @param fieldName underlying field this post-aggregator refers to (defaults to name)
druid.postagg.fieldAccess <- function(name, fieldName = name) {
  druid.build.postaggregator(type="fieldAccess", name=name, fieldName=fieldName)
}

#' Creates a fieldAccess post-aggregator
#' 
#' Defines a reference to a Druid field (e.g. an aggregator)
#' 
#' @param x name of the field
#' @export
field <- function(x) {
  druid.postagg.fieldAccess(as.character(x))
}

#' helper function to wrap constants in formulas
#' 
#' @param x numeric constant or post-aggregator
wrapConstant <- function(x) {
  if(is.numeric(x)) druid.postagg.constant(x)
  else x
}

#' Sum of Druid post-aggregators
#' 
#' @param ... post-aggregators
#' @export
#' @method sum druid.postaggregator
sum.druid.postaggregator <- function(...) {
  arglist <- list(...)
  # remove base function na.rm argument
  arglist <- arglist[!names(arglist) %in% "na.rm"]
  
  if(length(arglist) <= 1) {
    return(arglist[[1]])
  } else {
    return(arglist[[1]] + do.call("sum", arglist[-1]))
  }
}

#' Define a post-aggregator by multiplying aggregators, post-aggregators or constants
#' 
#' @param a aggregator, post-aggregator, or constant
#' @param b a aggregator, post-aggregator, or constant
#' @return post-aggregator a * b
#' @method * druid.postaggregator
#' @export
`*.druid.postaggregator` <- function(a, b) {
  druid.postagg.arithmetic("*", wrapConstant(a), wrapConstant(b))
}

#' Define a post-aggregator by dividing aggregators, post-aggregators or constants
#' 
#' @param a aggregator, post-aggregator, or constant
#' @param b a aggregator, post-aggregator, or constant
#' @return post-aggregator a / b
#' @method / druid.postaggregator
#' @export
`/.druid.postaggregator` <- function(a, b) {
  druid.postagg.arithmetic("/", wrapConstant(a), wrapConstant(b))
}

#' Define a post-aggregator by adding aggregators, post-aggregators or constants
#' 
#' @param a aggregator, post-aggregator, or constant
#' @param b a aggregator, post-aggregator, or constant
#' @return post-aggregator a + b
#' @method + druid.postaggregator
#' @export
`+.druid.postaggregator` <- function(a, b) {
  druid.postagg.arithmetic("+", wrapConstant(a), wrapConstant(b))
}

#' Define a post-aggregator by subtracting aggregators, post-aggregators or constants
#' 
#' @param a aggregator, post-aggregator, or constant
#' @param b a aggregator, post-aggregator, or constant
#' @return post-aggregator a - b
#' @method - druid.postaggregator
#' @export
`-.druid.postaggregator` <- function(a, b) {
  druid.postagg.arithmetic("-", wrapConstant(a), wrapConstant(b))
}
