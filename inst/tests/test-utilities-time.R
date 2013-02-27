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


test_that("fromISO parses milliseconds", {
  t <- fromISO("2011-05-01T02:03:00.123")
  expect_equal(second(t), 0.123, tolerance = 0.0001, scale=1)
  
  t <- fromISO("2011-05-01T02:03:00.123+05:30")
  expect_equal(second(t), 0.123, tolerance = 0.0001, scale=1)

  t <- fromISO("2011-05-01T02:03:00.001-05:30")
  expect_equal(second(t), 0.001, tolerance = 0.0001, scale=1)
})

test_that("fromISO parses TZ offset", {
  t_exp <-  as.POSIXct("2012-04-01", tz="UTC")

  t <- fromISO("2012-04-01T00:00:00")
  expect_equal(t, t_exp, tolerance=0.0001, scale=1)
  
  t <- fromISO("2012-04-01T00:00:00+00:00")
  expect_equal(t, t_exp, tolerance=0.0001, scale=1)
  
  t <- fromISO("2012-04-01T05:00:00+05:00")
  expect_equal(t, t_exp, tolerance=0.0001, scale=1)
  
  t <- fromISO("2012-04-01T05:30:00+05:30")
  expect_equal(t, t_exp, tolerance=0.0001, scale=1)

  t <- fromISO("2012-03-31T19:00:00-05:00")
  expect_equal(t, t_exp, tolerance=0.0001, scale=1)

  t <- fromISO("2012-03-31T18:30:00-05:30")
  expect_equal(t, t_exp, tolerance=0.0001, scale=1)  
})

test_that("fromISO properly works in vector format", {
  t     <- fromISO(   c("2012-04-01T00:00:00+00:00", "2012-04-01T00:00:00", "2012-04-01T00:00:00+05:00", "2012-04-01T00:00:00-05:30"))
  t_exp <- as.POSIXct(c("2012-04-01 00:00:00",       "2012-04-01 00:00:00", "2012-03-31 19:00:00",       "2012-04-01 05:30:00"), tz="UTC", format="%Y-%m-%d %H:%M:%S")
  expect_equal(t, t_exp, tolerance=0.0001, scale=1)
})

test_that("toISO prints milliseconds", {
  t <- as.POSIXct("2012-04-01", tz="UTC")
  t <- t + 0.125
  expect_identical("2012-04-01T00:00:00.125+00:00", toISO(t))
})

test_that("toISO prints interval", {
  t <- as.POSIXct("2012-01-01", tz="UTC")
  t1 <- c(t,             t + dminutes(30), t + dhours(14))
  t2 <- c(t + dhours(2), t + dhours(3),    t + ddays(2))  
  i <- interval(t1, t2)
  
  expect_identical(toISO(i), c("2012-01-01T00:00:00.000+00:00/2012-01-01T02:00:00.000+00:00",
                               "2012-01-01T00:30:00.000+00:00/2012-01-01T03:00:00.000+00:00",
                               "2012-01-01T14:00:00.000+00:00/2012-01-03T00:00:00.000+00:00"))
})
