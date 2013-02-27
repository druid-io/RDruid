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



druid.filter <- setClass("druid.filter", representation="list", S3methods=TRUE)

druid.build.filter <- function(type, ...) {
  druid.filter(list(type = type, ...))
}

# Functions to build filters

#' Construct a selector filter of the given named arguments
#' @param ... named arguments of selectors to match
#' @export
selector <- function(...) {
  x <- list(...)
  druid.filter.selector(names(x[1]), as.character(x[1]))
}

#' Construct a selector filter for a given dimension
#' @param a dimension to match
#' @param b value to match
#' @method == druid.dimension
#' @export
`==.druid.dimension` <- function(a, b) {
  if(is(a, "druid.dimension")) {dimension <- a; value <- b}
  else if(is(b, "druid.dimension")) {dimension <- b; value <- a}
  druid.filter.selector(dimension = as.character(dimension), value = as.character(value))
}

#' @name patternmatch
#' @export
`%=~%` <- function(dimension, pattern) {
  UseMethod("%=~%", dimension)
}

#' Construct a regex filter
#' 
#' @param dimension dimension to match
#' @param pattern pattern to match
#' @export
patternmatch <- function(dimension, pattern) {
  UseMethod("patternmatch", dimension)
}

#' Construct a regex filter for a given dimension
#' 
#' @param dimension dimension to match
#' @param pattern pattern to match
#' @method %=~% druid.dimension
#' @export
`%=~%.druid.dimension` <- function(dimension, pattern) {
  stopifnot(is(dimension, "druid.dimension"))
  druid.filter.regex(dimension = dimension, pattern = pattern)
}

#' @method != druid.dimension
#' @export
`!=.druid.dimension` <- function(a, b) {
  !(a == b)
}

druid.filter.selector <- function(dimension, value) {
  druid.build.filter("selector", dimension = dimension, value = value)
}

#' Construct an `and' filter of the given filters
#' @param ... filters to combine
#' @export
druid.filter.and <- function(...) {
  druid.build.filter("and", fields = list(...))
}

#' Construct an `or' filter of the given filters
#' @param ... filters to combine
#' @export
druid.filter.or <- function(...) {
  druid.build.filter("or", fields = list(...))
}

#' Construct a `not' filter of the given filter
#' @param filter to negate
druid.filter.not <- function(filter) {
  druid.build.filter("not", field = filter)
}

#' @method & druid.filter
#' @export
`&.druid.filter` <- function(a, b) {
  druid.filter.and(a, b)
}

#' @method | druid.filter
#' @export
`|.druid.filter` <- function(a, b) {
  if(a$type == "or") {
    do.call("druid.filter.or", c(a$fields, list(b)))
  }
  else if(b$type == "or") {
    do.call("druid.filter.or", c(list(a), b$fields))
  }
  else {
    druid.filter.or(a, b)
  }
}

#' @method ! druid.filter
#' @export
`!.druid.filter` <- function(x) {
  druid.filter.not(x)
}

druid.filter.extraction <- function(dimension, value, dimExtractionFn) {
  druid.build.filter("extraction", dimension = dimension, value = value, dimExtractionFn = dimExtractionFn)
}

#' Construct a regex filter for a given dimension
#' 
#' @param dimension dimension to match
#' @param pattern pattern to match
#' @export
druid.filter.regex <- function(dimension, pattern) {
  druid.build.filter("regex", dimension = dimension, pattern = pattern)
}

#' @method toString druid.filter
#' @export
toString.druid.filter <- function(x, ...) {
  switch(x$type,
         regex    = paste(x$dimension, " =~ ", "/", x$pattern, "/", sep=""),
         selector = paste(x$dimension, "==", x$value),
         and      = paste("(", do.call("paste", c(llply(x$fields, toString), list(sep=" && "))), ")"),
         or       = paste("(", do.call("paste", c(llply(x$fields, toString), list(sep=" || "))), ")"),
         not      = paste("!(", toString(x$field), ")"),
  )
}

#' @method print druid.filter
#' @export
print.druid.filter <- function(x, ...) {
  cat("druid filter expression: ", toString(x, ...), "\n", sep="")
}
