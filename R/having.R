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



setClass("druid.having", representation = "list", S3methods = TRUE)
druid.having <- function(...) new("druid.having", ...)

druid.build.having <- function(type, ...) {
  druid.having(list(type = type, ...))
}

# Functions to build having clauses (http://druid.io/docs/latest/querying/having.html)

#' Construct an equalTo having clause of the given named arguments
#' @param ... named arguments of equalTos to match
#' @export
equalTo <- function(...) {
  x <- list(...)
  druid.having.equalTo(names(x[1]), as.character(x[1]))
}

#' Construct an equalTo having clause for a given aggregation
#' @param a aggregation to match
#' @param b value to match
#' @method == druid.aggregation
#' @export
`==.druid.aggregation` <- function(a, b) {
  if(is(a, "druid.aggregation")) {aggregation <- a; value <- b}
  else if(is(b, "druid.aggregation")) {aggregation <- b; value <- a}
  druid.having.equalTo(aggregation = as.character(aggregation), value = as.numeric(value))
}

druid.having.equalTo <- function(aggregation, value) {
  druid.build.having("equalTo", aggregation = aggregation, value = value)
}

#' Construct a greaterThan having clause for a given aggregation
#' @param a aggregation to match
#' @param b value to match
#' @method >= druid.aggregation
#' @export
`>=.druid.aggregation` <- function(a, b) {
  if(is(a, "druid.aggregation")) {aggregation <- a; value <- b}
  else if(is(b, "druid.aggregation")) {aggregation <- b; value <- a}
  druid.having.greaterThan(aggregation = as.character(aggregation), value = as.numeric(value))
}

druid.having.greaterThan <- function(aggregation, value) {
  druid.build.having("greaterThan", aggregation = aggregation, value = value)
}

#' Construct a lessThan having clause for a given aggregation
#' @param a aggregation to match
#' @param b value to match
#' @method <= druid.aggregation
#' @export
`<=.druid.aggregation` <- function(a, b) {
  if(is(a, "druid.aggregation")) {aggregation <- a; value <- b}
  else if(is(b, "druid.aggregation")) {aggregation <- b; value <- a}
  druid.having.lessThan(aggregation = as.character(aggregation), value = as.numeric(value))
}

druid.having.lessThan <- function(aggregation, value) {
  druid.build.having("lessThan", aggregation = aggregation, value = value)
}

#' Construct an `and' having of the given havingspecs
#' @param ... havings to combine
#' @export
druid.having.and <- function(...) {
  druid.build.having("and", havingSpecs = list(...))
}

#' Construct an `or' having of the given havingspecs
#' @param ... havings to combine
#' @export
druid.having.or <- function(...) {
  druid.build.having("or", havingSpecs = list(...))
}

#' Construct a `not' having of the given havingspec
#' @param having to negate
druid.having.not <- function(having) {
  druid.build.having("not", havingSpec = having)
}

#' @method & druid.having
#' @export
`&.druid.having` <- function(a, b) {
  druid.having.and(a, b)
}

#' @method | druid.having
#' @export
`|.druid.having` <- function(a, b) {
  if(a$type == "or") {
    do.call("druid.having.or", c(a$fields, list(b)))
  }
  else if(b$type == "or") {
    do.call("druid.having.or", c(list(a), b$fields))
  }
  else {
    druid.having.or(a, b)
  }
}

#' @method ! druid.having
#' @export
`!.druid.having` <- function(x) {
  druid.having.not(x)
}

#' @method toString druid.having
#' @export
toString.druid.having <- function(x, ...) {
  switch(x$type,
         lessThan     = paste(x$aggregation, "<=", x$value),
         greaterThan  = paste(x$aggregation, ">=", x$value),
         equalTo      = paste(x$aggregation, "==", x$value),
         and          = paste("(", do.call("paste", c(plyr::llply(x$fields, toString), list(sep=" && "))), ")"),
         or           = paste("(", do.call("paste", c(plyr::llply(x$fields, toString), list(sep=" || "))), ")"),
         not          = paste("!(", toString(x$field), ")"),
  )
}

#' @method print druid.having
#' @export
print.druid.having <- function(x, ...) {
  cat("druid having expression: ", toString(x, ...), "\n", sep="")
}
