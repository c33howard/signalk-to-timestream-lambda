/*
 * Copyright 2020 Craig Howard <craig@choward.ca>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const aws = require('aws-sdk');
const csv = require('fast-csv');
const zlib = require('zlib');
const _ = require('lodash');

exports.handler = async (event) => {
    const self_id = process.env.SELF_ID;
    const s3 = new aws.S3();

    const database_name = process.env.DATABASE_NAME;
    const table_name = process.env.TABLE_NAME;
    const should_delete = process.env.DELETE_FROM_S3_ON_SUCCESS;
    // TODO: couldn't get this working with aws-sdk-v3
    const timestream_write = new aws.TimestreamWrite();

    console.log('starting');

    let _get_from_s3 = function() {
        const bucket = event.Records[0].s3.bucket.name;
        const key = decodeURIComponent(event.Records[0].s3.object.key);
        console.log(`get_object ${bucket}:${key}`);

        const get_object_command = {
            Bucket: bucket,
            Key: key
        };

        return s3.getObject(get_object_command).createReadStream();
    };

    let _delete_from_s3 = function() {
        const bucket = event.Records[0].s3.bucket.name;
        const key = decodeURIComponent(event.Records[0].s3.object.key);
        console.log(`delete_object ${bucket}:${key}`);

        const delete_object_command = {
            Bucket: bucket,
            Key: key
        };

        return s3.deleteObject(delete_object_command).createReadStream();
    };

    let _parse_csv = function(body) {
        return new Promise(function(resolve, reject) {
            const unzipped_stream = body.pipe(zlib.createGunzip());
            const row_stream = unzipped_stream.pipe(csv.parse({ headers: true }));

            let rows = [];
            row_stream.on('error', reject);
            row_stream.on('data', row => rows.push(row));
            row_stream.on('end', () => resolve(rows));
        });
    };

    let _value_to_type = function(value) {
        if (!Number.isNaN(Number(value))) {
            return "DOUBLE";
        } else if (value === 'true' || value === 'false') {
            return "BOOLEAN";
        } else {
            return "VARCHAR";
        }
    };

    let _write_to_timestream = function(rows) {
        // start with a list of points in timestream format for each row
        let records = rows.map(function(row) {
            // pull out the measure name
            const name = row.path;
            const data = _.omit(row, ['path']);

            // remaining points are time: value pairs, map each to timestream format
            return _.toPairs(data).map(function(pair) {
                const value_type = _value_to_type(pair[1]);
                const result = {
                    MeasureName: name,
                    MeasureValue: pair[1],
                    MeasureValueType: value_type,
                    Time: pair[0]
                };
                return result;
            });
        })

        // flatten the list to a single list of points
        records = records.flat();
        // filter out any missing values (possible if a row is incomplete, due
        // to a device being turned on/off in the middle of an update interval)
        records = records.filter(measure => measure.MeasureValue != '');

        // timestream requires we submit batches of 100 data points, so chunk
        // the records
        const record_chunks = _.chunk(records, 100);

        console.log(`going to upload ${record_chunks.length} batches to timestream`);

        // make a call (and promise) for each chunk
        const promises = record_chunks.map(chunk => {
            const write_records_command = {
                DatabaseName: database_name,
                TableName: table_name,
                Records: chunk,
                CommonAttributes: {
                    TimeUnit: 'MILLISECONDS',
                    Dimensions: [{
                        Name: 'context',
                        Value: self_id
                    }]
                }
            };

            return new Promise((resolve, reject) => {
                timestream_write.writeRecords(write_records_command, function(err, data) {
                    if (err) {
                        console.log(err, data);
                        console.log(err.RejectedRecords);
                        reject(err);
                    }
                    else resolve(data);
                });
            });
        });

        return Promise.all(promises);
    };
    
    return new Promise(async function(resolve, reject) {
        try {
            const s3_result = _get_from_s3();
            console.log(`done: get object from s3`);

            const rows = await _parse_csv(s3_result);
            const ts = await _write_to_timestream(rows);

            console.log(`done: uploaded ${ts.length} batches to timestream`);

            if (should_delete) {
                await _delete_from_s3();
                console.log(`done: delete object from s3`);
            }

            resolve('ok');
        } catch (err) {
            reject(err);
        }
    });
};
