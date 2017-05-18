#! /usr/bin/env bash

main() {
    ant runtest -Dtest=BTreeFileReadTest
    ant runsystest -Dtest=BTreeScanTest

    ant runtest -Dtest=BTreeFileInsertTest
    ant runsystest -Dtest=BTreeFileInsertTest

    ant runtest -Dtest=BTreeFileDeleteTest
    ant runsystest -Dtest=BTreeFileDeleteTest
}

main "$@"