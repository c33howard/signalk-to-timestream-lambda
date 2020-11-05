# Lambda Function for Signalk CSV to Timestream

This is the lambda function I use for processing the CSVs uploaded to S3 by
signalk-to-csv and then writing to Amazon Timestream.

See signalk-to-csv for more detail on the setup and motivation.

# Setup

First, do an npm install in this directory, to get all the dependencies.  This
might be cleanest in a docker instance:

```
docker run -v $(pwd):/code node:12 bash
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
