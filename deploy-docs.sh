#!/bin/sh

TARGET=deploy/docs
if [ -d $TARGET ] ; then
  git --git-dir=$TARGET/.git pull
else
  mkdir -p $TARGET
  git clone -b gh-pages git@github.com:druid-io/RDruid.git $TARGET
fi

rm -rf $TARGET/*.html $TARGET/css $TARGET/icons $TARGET/img $TARGET/js
Rscript -e 'library(methods); staticdocs::build_package(".", "./deploy/docs")'

git --git-dir=$TARGET/.git --work-tree=$TARGET add -A
