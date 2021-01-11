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
const from_batch = require('signalk-batcher').from_batch;
const zlib = require('zlib');
const _ = require('lodash');

exports.handler = async (event) => {
    const self_id = process.env.SELF_ID;
    const s3 = new aws.S3();

    const database_name = process.env.DATABASE_NAME;
    const table_name = process.env.TABLE_NAME;
    const should_delete = process.env.DELETE_FROM_S3_ON_SUCCESS === "yes";
    // TODO: couldn't get this working with aws-sdk-v3
    const timestream_write = new aws.TimestreamWrite();

    const sns_message = event.Records[0].Sns.Message;
    const s3_record = JSON.parse(sns_message).Records[0].s3;
    const s3_bucket = s3_record.bucket.name;
    const s3_key = decodeURIComponent(s3_record.object.key);

    console.log('starting');

    let _get_from_s3 = function() {
        console.log(`get_object ${s3_bucket}:${s3_key}`);

        const get_object_command = {
            Bucket: s3_bucket,
            Key: s3_key
        };

        return s3.getObject(get_object_command).createReadStream();
    };

    let _delete_from_s3 = function() {
        console.log(`delete_object ${s3_bucket}:${s3_key}`);

        const delete_object_command = {
            Bucket: s3_bucket,
            Key: s3_key
        };

        return s3.deleteObject(delete_object_command).createReadStream();
    };

    let _parse_json = function(body) {
        return new Promise(function(resolve, reject) {
            const unzipped_stream = body.pipe(zlib.createGunzip());

            let str = '';
            unzipped_stream.on('error', reject);
            unzipped_stream.on('data', data => str += data);
            unzipped_stream.on('end', () => resolve(JSON.parse(str)));
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

    let _write_to_timestream = function(points) {
        const records = points.map(function(item) {
            const value_type = _value_to_type(item.value);
            // cast to Number, which is IEEE 754, so Timestream accepts this,
            // probably redundant, but I saw some weird errors about number
            // format
            if (value_type === "DOUBLE") {
                item.value = Number.parseFloat(item.value);
            }

            return {
                MeasureName: item.path,
                MeasureValue: `${item.value}`,
                MeasureValueType: _value_to_type(item.value),
                Time: `${item.time}`,
                Dimensions: [{
                    Name: 'source',
                    Value: item.$source,
                    DimensionValueType: 'VARCHAR'
                }]
            };
        });

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

            const state = await _parse_json(s3_result);
            const points = from_batch(state);
            const ts = await _write_to_timestream(points);

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
