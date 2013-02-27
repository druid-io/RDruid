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


#' calls toJSON with a default set of arguments
#' 
#' - sets the default number of digits to 22 to retain maximum precisioin
#' - removes newlines and spaces from json string
#' 
#' @param obj object to pass to toJSON
#' @param ... other arguments to pass to toJSON
json <- function(obj, ...) {
  toJSON(obj, digits=22, collapse="", ...)
}

#' Send a JSON query to Druid
#'
#' Takes a JSON string and uses RCurl to send a query to the endpoint
#' specified by url.  Typically this function is not called on its own,
#' instead the end user should use the query family of functions
#'
#' @param jsonstr JSON string containing details of the query.
#' @param url the endpoint for where this query should be sent. Use druid.url()
#'   to construct the URL.
#' @keywords database, druid, query
#' @seealso \code{\link{druid.query.timeseries}}
query <- function(jsonstr, url){
    h <- basicTextGatherer()
    tryCatch({
        if(is.null(jsonstr)) {
          curlPerform(url = url,
                      writefunction = h$update,
                      .encoding = "UTF-8")
        } else {
          curlPerform(postfields = jsonstr,
                      httpheader = "Content-Type: application/json",
                      url = url,
                      writefunction = h$update,
                      .encoding = "UTF-8")
        }
        h$value()
    }, warning = function(war) {
        warning(war)
    }, error = function(err) {
        stop(err)
    }, finally = {

    })
}

