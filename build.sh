#!/usr/bin/env bash
# build the logstash project

BUILD_DIR=".tmp"
GEMSPEC="logstash-output-ceph.gemspec"

INCLUDE_FILES=(
${GEMSPEC}
"Rakefile"
"Gemfile"
"lib"
)

if [ "`which jrubyc`" = "" ]; then
    echo "To build the project, you must have jrubyc in the path."
    exit -1
fi

if [ -d ${BUILD_DIR} ]; then
    rm .tmp -rf
fi

mkdir ${BUILD_DIR}

for f in $INCLUDE_FILES; do
    cp -r $f ${BUILD_DIR}
done

pushd ${BUILD_DIR}
    find . -name "*.rb" -exec jrubyc {} \;
    gem build ${GEMSPEC}
popd
