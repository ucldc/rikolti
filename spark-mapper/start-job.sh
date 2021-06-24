if [[ -n "$DEBUG" ]]; then
  set -x
fi

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # http://stackoverflow.com/questions/59895
cd $DIR

if [ $# -ne 2 ];
  then
    echo "start-job.sh <job name> <collection-id>"
    exit 1
fi

set -u

aws glue start-job-run --job-name $1 --arguments="--collection_id=$2"

# aws glue get-job-runs --job-name nuxeo-mapper
# lando gluesparksubmit nuxeo_mapper.py --JOB_NAME=nux --collection_id=26697