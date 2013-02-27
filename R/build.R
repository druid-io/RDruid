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


# Druid metric and dimension objects

druid.metric     <- setClass("druid.metric"   , representation="character", S3methods=TRUE)
druid.dimension  <- setClass("druid.dimension", representation="character", S3methods=TRUE)

#' Defines a reference to a Druid metric
#' 
#' @param x name of the metric
#' @export
metric <- function(x) {
  druid.metric(as.character(x))
}

#' @method print druid.metric
#' @export
print.druid.metric <- function(x, ...) {
  cat("druid metric: ", toString(x), "\n", sep="")
}

#' Creates a Druid dimension object
#' 
#' @param name dimension name
#' @return a Druid dimension object
#' @export
dimension <- function(name) {
  druid.dimension(as.character(name))
}

#' @method print druid.dimension
#' @export
print.druid.dimension <- function(x, ...) {
  cat("druid dimension: ", toString(x), "\n", sep="")
}
