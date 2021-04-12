# Getting Started with Local Development:

1. Download lando: https://lando.dev/download/
2. Get temporary credentials using AWS STS
3. Set environmental variables in local.env
4. From this directory, run `lando start`

## Helpful Lando Commands

`lando gluepyspark` - runs pyspark with glue libraries available
`lando gluesparksubmit` - submits a job to glue
`lando gluepytest` - runs gluepytest
`lando ssh glue` - ssh to the container
`lando list` - lists all running lando apps and containers
`lando stop` - stop the glue app
`lando poweroff` - stop all lando related containers

# Run a job :tada:

`lando gluesparksubmit pyspark-oac-mapper.py`
