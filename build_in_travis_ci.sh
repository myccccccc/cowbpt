if [ -z "$PURPOSE" ]; then
    echo "PURPOSE must be set"
    exit 1
fi
if [ -z "$CXX" ]; then
    echo "CXX must be set"
    exit 1
fi
if [ -z "$CC" ]; then
    echo "CC must be set"
    exit 1
fi

runcmd(){
    eval $@
    [[ $? != 0 ]] && {
        exit 1
    }
    return 0
}

echo "build combination: PURPOSE=$PURPOSE CXX=$CXX CC=$CC"

rm -rf build && mkdir build && cd build
if [ "$PURPOSE" = "compile" ]; then
    if ! cmake .. -DCOWBPT_BUILD_TESTS=OFF; then
        echo "Fail to generate Makefile by cmake"
        exit 1
    fi
    make -j4
elif [ "$PURPOSE" = "unittest" ]; then
    if ! cmake ..; then
        echo "Fail to generate Makefile by cmake"
        exit 1
    fi
    make -j4 && ./runUnitTests
fi