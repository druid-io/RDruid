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


setClass("druid.granularity" , representation="character", S3methods=TRUE)
druid.granularity <- function(...) new("druid.granularity", ...)

granularity.period <- function(period, origin = NA, timeZone = NA) {
  structure(list(type = "period", period=period, origin=origin, timeZone=timeZone), class="druid.granularity")
}

granularity.duration <- function(duration, origin = NA) {
  structure(list(type = "duration", duration=duration, origin=origin), class="druid.granularity")
}

granularity.string <- function(name) {
  structure(name, class="druid.granularity")
}

#' Creates a Druid query granularity object
#' 
#' Specifies a Druid query granularity using a simple string, a numerical duration or an ISO period
#' 
#' @details
#' Granularities can be specified in several ways:
#' 
#' - as a simple string: "day", "hour", "minute", "all", "none"
#' 
#' - as a number, representing the number of seconds in each period
#' 
#' - as an ISO period, such as "P1M", "P1W", PT1H", etc.
#' 
#' @examples
#' 
#' granularity("day")
#' 
#' granularity("PT6H", timeZone="America/Los_Angeles")
#' 
#' granularity(3600 * 2, origin=ymd_hms("2013-01-01T01:00:00"))
#' 
#' @param str a string or number representing a granularity (e.g. "day", "hour", "minute", "all", "none", 3600, "P1M", "PT1H")
#' @param period granularity specified as an ISO period
#' @param duration granularity specified as a duration in seconds
#' @param origin timestamp to start computing intervals from
#' @param timeZone optional time zone to define period granularities specified
#'        as a IANA time zone string, e.g. "America/Los_Angeles"
#' 
#' @return a Druid granularity object
#' @export
granularity <- function(str=NA, period=NA, duration=NA, origin=NA, timeZone=NA) {
  
  if(str %in% c("day", "hour", "minute", "second", "all", "none")) {
    return(granularity.string(str))
  }
  
  hasPeriod <- !is.na(period) || (!is.na(str) && substr(str, 0, 1) == "P")
  hasDuration <- !is.na(duration) || (!is.na(str) && is.numeric(str))
  
  if(is.na(period)) period <- str
  if(is.na(duration)) duration <- str
  
  if(!is.na(origin)) {
    origin <- toISO(origin)
  }

  if(!xor(hasPeriod,hasDuration)) {
    stop("Must specify either duration or period")
  }
  
  if(hasPeriod) {
    return(granularity.period(period=period, origin=origin, timeZone=timeZone))
  }
  
  if(hasDuration) {
    return(granularity.duration(duration=duration, origin=origin))
  }
  
  stop(sprintf("Unknown granularity [%s]", str))
}

#' @method toString druid.granularity
#' @export
toString.druid.granularity <- function(x, ...) {
  if(is.character(x)) {
    return(x)
  }
  switch(x$type,
         period = paste("period:", x$period, "origin:", x$origin, "timeZone:", x$timeZone),
         duration = paste("duration:", x$duration, "origin:", x$origin)
  )
}

#' @method print druid.granularity
#' @export
print.druid.granularity <- function(x, ...) {
  cat("druid granularity: ", toString(x), "\n", sep="")
}
