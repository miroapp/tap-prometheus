#!/usr/bin/env python3

from datetime import datetime
import pytz
import os
import json

import singer
from singer import utils, Transformer
from singer import metadata

from promalyze import Client

REQUIRED_CONFIG_KEYS = ['endpoint', 'start_date', 'metrics']
STATE = {}

LOGGER = singer.get_logger()

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class Context:
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}
    new_counts = {}
    updated_counts = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map.get(stream_name)

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if stream is not None:
            stream_metadata = metadata.to_map(stream['metadata'])
            return metadata.get(stream_metadata, (), 'selected')
        return False

    @classmethod
    def print_counts(cls):
        LOGGER.info('------------------')
        for stream_name, stream_count in Context.new_counts.items():
            LOGGER.info('%s: %d new, %d updates',
                        stream_name,
                        stream_count,
                        Context.updated_counts[stream_name])
        LOGGER.info('------------------')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            # TODO Events may have a different key property than this. Change
            # if it's appropriate.
            'key_properties': ['date', 'metric', 'aggregation']
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def sync(client):
    # Write all schemas and init count to 0
    for catalog_entry in Context.catalog['streams']:
        stream_name = catalog_entry["tap_stream_id"]
        singer.write_schema(stream_name, catalog_entry['schema'], catalog_entry['key_properties'])

        Context.new_counts[stream_name] = 0
        Context.updated_counts[stream_name] = 0

    for metric in Context.config['metrics']:
        name = metric['name']
        query = metric['query']
        aggregation = metric['aggregation']
        period = metric['period']
        step = metric['step']

        LOGGER.info('Loading metric "%s" using query "%s", aggregation: %s, period: %s, metric step: %d seconds',
                    name, query, aggregation, period, step)

        query_metric(client, name, query, aggregation, period, step)


def query_metric(client, name, query, aggregation, period, step):
    stream_name = 'aggregated_metric_history'
    catalog_entry = Context.get_catalog_entry(stream_name)
    stream_schema = catalog_entry['schema']

    bookmark = get_bookmark(name)

    bookmark_unixtime = int(datetime.strptime(bookmark, DATE_FORMAT).replace(tzinfo=pytz.UTC).timestamp())
    extraction_time = singer.utils.now()
    current_unixtime = int(extraction_time.timestamp())

    if period == 'day':
        period_seconds = 86400
    else:
        raise Exception("Period is not supported: " + period)

    iterator_unixtime = bookmark_unixtime
    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
        while iterator_unixtime + period_seconds <= current_unixtime:
            ts_data = client.range_query(
                query,
                start=iterator_unixtime,
                end=iterator_unixtime + period_seconds,
                step=step
            )  # returns PrometheusData object
            # TODO hande empty array
            ts = ts_data.timeseries[0]  # returns a TimeSeries object

            dataframe = ts.as_pandas_dataframe()
            dataframe['values'] = dataframe['values'].astype(float)

            aggregated_value = aggregate(aggregation, dataframe)

            # print(" " + str(bookmark_unixtime) + " "+ str(aggregated_value))
            data = {
                "date": iterator_unixtime,
                "metric": name,
                "aggregation": aggregation,
                "value": aggregated_value
            }
            rec = transformer.transform(data, stream_schema)

            singer.write_record(
                stream_name,
                rec,
                time_extracted=extraction_time
            )

            Context.new_counts[stream_name] += 1

            singer.write_bookmark(
                Context.state,
                name,
                'start_date',
                datetime.utcfromtimestamp(iterator_unixtime + period_seconds).strftime(DATE_FORMAT)
            )

            # write state after every 100 records
            if (Context.new_counts[stream_name] % 100) == 0:
                singer.write_state(Context.state)

            iterator_unixtime += period_seconds

    singer.write_state(Context.state)


def aggregate(aggregation, dataframe):
    if aggregation == 'max':
        aggregated_value = dataframe.max()['values']
    elif aggregation == 'min':
        aggregated_value = dataframe.min()['values']
    elif aggregation == 'avg':
        aggregated_value = dataframe.mean()['values']
    else:
        raise Exception("Aggregation method not implemented: " + aggregation)
    return aggregated_value


def get_bookmark(name):
    bookmark = singer.get_bookmark(Context.state, name, 'start_date')
    if bookmark is None:
        bookmark = Context.config['start_date']
    return bookmark


def init_prom_client():
    return Client(Context.config['endpoint'])


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))

    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()

        Context.config = args.config
        Context.state = args.state

        client = init_prom_client()
        sync(client)


if __name__ == '__main__':
    main()
