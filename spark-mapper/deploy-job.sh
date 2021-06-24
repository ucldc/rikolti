if [[ -n "$DEBUG" ]]; then
  set -x
fi

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # http://stackoverflow.com/questions/59895
cd $DIR

if [ $# -ne 1 ];
  then
    echo "deploy-job.sh <module name>"
    exit 1
fi

set -u

MODULE="$1.py"

aws s3 cp $MODULE s3://$S3_BUCKET/deployments/metadata-mapper/$MODULE

