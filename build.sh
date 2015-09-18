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

pushd () {
    command pushd "$@" > /dev/null
}

popd () {
    command popd "$@" > /dev/null
}

if [ "`which jrubyc`" = "" ]; then
    echo "To build the project, you must have jrubyc in the path."
    exit -1
fi

if [ -d ${BUILD_DIR} ]; then
    rm .tmp -rf
fi

mkdir ${BUILD_DIR}

for f in ${INCLUDE_FILES[@]}; do
    cp -r $f ${BUILD_DIR}
done

pushd ${BUILD_DIR}
    find . -name "*.rb" | xargs -I{} sh -c "jrubyc {}; rm {}"
    [ "`which tree`" != "" ] && tree
    echo "# build the gem" > build.log
    gem build ${GEMSPEC} &>> build.log
    GEMFILE=`ls *gem`
    mv ${GEMFILE} build.log ..
popd

if [ -f "$GEMFILE" ]; then
    echo -e "\033[32mHave generate the gem file: $GEMFILE\033[0m"
else
    echo -e "\033[31mFailed to generate the gem file, you may check the build.log.\033[0m"
fi

rm ${BUILD_DIR} -rf
