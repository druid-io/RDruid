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

test_that("groupBy handles missing columns", {
  res <- fromJSON('[{"timestamp":"2013-07-01T00:00:00.000Z","version":"v1","event":{"count":10,"col1":"No","col2":"Yes"}},{"timestamp":"2013-07-01T00:00:00.000Z","version":"v1","event":{"count":20,"col1":"Yes"}}]',
                  simplifyVector = F)
  df <- RDruid:::druid.groupBytodf(res)
  expected <- data.frame(
    timestamp = fromISO(c("2013-07-01T00:00:00.000Z", "2013-07-01T00:00:00.000Z")),
    count = c(10, 20),
    col1  = c("No", "Yes"),
    col2  = c("Yes", NA),
    stringsAsFactors=F
  )
  expect_equal(df, expected)
})

