#!/bin/sh

clean=`git status | grep -q "nothing to commit"`
if (( !clean )); then
    git stash -u
fi

poetry build -f wheel

git push origin
git push origin v$1
gh release create v$1 --verify-tag --title "Release $1" --notes ""
gh release upload v$1 dist/flatmapknowledge-$1-py3-none-any.whl

if (( !clean )); then
    git stash pop --quiet
fi
