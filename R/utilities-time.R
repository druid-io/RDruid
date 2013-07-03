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


#' Convert an object to its ISO 8601 string representation
#'
#' @param t object to be converted
#' @export
toISO <- function(t) {
  UseMethod("toISO", t)
}

#' toISO leaves strings as is without checking for ISO 8601 validity
#' 
#' @param t ISO 8601 character vector
#' @method toISO character
#' @export
toISO.character <- function(t) {
  t
}

#' Convert a POSIX timestamp to its ISO 8601 string representation
#' 
#' @param t object to be converted
#' @method toISO numeric
#' @export
toISO.numeric <- function(t) {
  toISO(as.POSIXct(t, origin=origin))
}

#' Convert a POSIX* object to its ISO 8601 string representation
#'
#' @param t object to be converted
#' @method toISO POSIXt
#' @export
toISO.POSIXt <- function(t) {
  # make sure milliseconds are properly written out
  strftime(t, "%Y-%m-%dT%H:%M:%OS3+00:00", tz="UTC")
}

#' Convert a Interval object to its ISO 8601 string representation
#'
#' @param t object to be converted
#' @method toISO Interval
#' @export
toISO.Interval <- function(t) {
  paste(toISO(t@start), toISO(t@start + t@.Data), sep="/")
}

#' Convert an ISO 8601 string to a POSIXct object
#'
#' @param x character string to be converted
#' @export
fromISO <- function(x) {
  tzoffset <- regexpr("([+-])(\\d\\d):(\\d\\d)$", x, perl=TRUE)
  i <- attr(tzoffset, "capture.start")
  l <- attr(tzoffset, "capture.length")
  sign <- as.numeric(substring(x, i[,1], i[,1] + l[,1] - 1) == "+") * 2 -1
  hours   <- as.numeric(substring(x, i[,2], i[,2] + l[,2] - 1))
  minutes <- as.numeric(substring(x, i[,3], i[,3] + l[,3] - 1))
  hours[is.na(hours)] <- 0
  minutes[is.na(minutes)] <- 0

  t <- as.POSIXct(x, format="%Y-%m-%dT%H:%M:%OS", tz="UTC")
  t - ( dhours(hours) + dminutes(minutes) ) * sign
}
