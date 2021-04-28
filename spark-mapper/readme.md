# Getting Started with Local Development:

1. Download lando: https://lando.dev/download/
2. From this directory, `cp local.env env.local`
3. Get temporary credentials using AWS STS
4. Set environmental variables in `env.local`
5. From this directory, run `lando start`

## Helpful Lando Commands

* `lando gluepyspark` - runs pyspark with glue libraries available
* `lando gluesparksubmit` - submits a job to glue
* `lando gluepytest` - runs gluepytest
* `lando ssh glue` - ssh to the container
* `lando list` - lists all running lando apps and containers
* `lando stop` - stop the glue app
* `lando poweroff` - stop all lando related containers
* `lando destroy` - when you need new AWS credentials, you'll need to destroy this app and then run `lando start` after updating `env.local`

# Run a job :tada:

`lando gluesparksubmit pyspark-oac-mapper.py`
