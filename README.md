# Lambda Function for Signalk Batch Format to Timestream

This is the lambda function I use for processing the batch jsons uploaded to S3
by [signalk-to-batch-format] and then writing to Amazon Timestream.

See [signalk-to-batch-format] for more detail on the setup and motivation.

# Setup

First, do an npm install in this directory, to get all the dependencies.  This
might be cleanest in a docker instance:

```
docker run -it -v $(pwd):/code node:12 bash
cd /code
npm install
```

Then, from outside docker, and with proper AWS lambda credentials configured,
just run make:

```
make
```

Note, that if you want to name your lambda something other than
signalk-to-timestream, edit the Makefile.

The lambda you create needs a role with the following permissions:

* S3:GetObject
* Timestream:DescribeEndpoints
* Timestream:WriteRecords
* Logs:CreateLogGroup
* Logs:CreateLogStream
* Logs:PutLogEvents

The logs permissions are optional.

In the lambda, set the following environment variables:

* DATABASE_NAME Your Timestream Database Name
* TABLE_NAME Your Timestream Table Name
* SELF_ID Your self id string from signalk (ie: urn:mrn:imo:mmsi:1234)
* DELETE_FROM_S3_ON_SUCCESS (Optional) the string "yes" or "no" to indicate whether this script should do the delete from S3

Depending on how many metrics you're publishing to timestream, you may need to
slightly increase the RAM and/or timeout of your lambda.  I'm running with
192MB of RAM and a 10s timeout.

[signalk-to-batch-format]: https://github.com/c33howard/signalk-to-batch-format
