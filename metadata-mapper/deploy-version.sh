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
    echo "deploy-version.sh YYYY-MM-DD-version-label"
    exit 1
fi

set -u

ZIP="fetcher-$1.zip"

#package the app and upload to s3
pip install --target ./package -r requirements.txt
cd package
zip -r ../$ZIP .
cd ..
zip -g $ZIP *.py
aws s3 cp $ZIP s3://$S3_BUCKET/deployments/metadata-mapper/$ZIP
rm -r package

aws lambda update-function-code \
  --function-name map-metadata \
  --s3-bucket $S3_BUCKET \
  --s3-key deployments/metadata-mapper/$ZIP \
  --region us-west-2

rm $ZIP


