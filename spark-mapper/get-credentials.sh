if [[ -n "$DEBUG" ]]; then
  set -x
fi

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # http://stackoverflow.com/questions/59895
cd $DIR

(aws sts assume-role \
  --role-arn $AMZ_GLUE_ARN \
  --role-session-name glue-dev \
  --profile gluedev \
  --duration-seconds 28800 \
  | jq --raw-output '"AWS_REGION=us-west-2", "AWS_ACCESS_KEY_ID="+.Credentials.AccessKeyId, "AWS_SECRET_ACCESS_KEY=" + .Credentials.SecretAccessKey, "AWS_SESSION_TOKEN="+.Credentials.SessionToken'
) > env.local

lando rebuild