#!/bin/bash -eux

cd $(readlink -m $0/../..)
test -f setup.py

rpmbuild \
    --define "pkgversion $(python setup.py --version)" \
    --define "_topdir ${PWD}/dist/rpm" \
    --define "_sourcedir ${PWD}/dist" \
    -ba rpm/temboard-sched.spec
