#!/bin/bash
#
set -ue

ver="$( echo "$*" | egrep -- '-V'\|'--version=' >/dev/null; echo $? )"
app="$( echo "$*" | egrep -- '-A'\|'--application=' >/dev/null; echo $? )"
if [ "$ver" != 0 -o "$app" != 0 ]
then
  cat <<EOD

ERROR: Must specify explict '-A <application> -V <version>' arguments, e.g.

   $0 -A <application> -V <version>

EOD
  exit 1
fi

echo -e "\n*** Rolling back any pending updates (just in case) ***\n"
appcfg.py --oauth2 $* rollback .

echo -e "\n*** DEPLOYING ***\n"
appcfg.py --oauth2 $* update .
