#!/usr/bin/env python3
import os
import json
import datetime
from datetime import timedelta

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_tapfiliate.tapfiliate_client import TapfiliateRestApi

REQUIRED_CONFIG_KEYS = ["x-api-token", "date_from", "page_offset_percentage", "date_offset_days"]

LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """Load schemas from schemas folder"""
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_bookmark(stream_id):
    bookmark = {
        "affiliate-groups": "page",
        "affiliate-prospects": "page",
        "affiliates": "page",
        # "balances": "page",
        "commissions": "page",
        "conversions": "date_from",
        "customers": "date_from",
        # "payments": "page",
        "programs": "page",
    }
    return bookmark.get(stream_id)


def get_key_properties(stream_id):
    key_properties = {
        "affiliate-groups": ["id"],
        "affiliate-prospects": ["id"],
        "affiliates": ["id"],
        # "balances": ["id"],
        "commissions": ["id"],
        "conversions": ["id"],
        "customers": ["id"],
        # "payments": ["id"],
        "programs": ["id"],
    }
    return key_properties.get(stream_id, [])


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [
        {
            "breadcrumb": [],
            "metadata": {
                "inclusion": "available",
                "forced-replication-method": "INCREMENTAL",
                "valid-replication-keys": [replication_key],
            },
        }
    ]
    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if replication_key is None:
        mdata[0]["metadata"]["forced-replication-method"] = "FULL_TABLE"
        mdata[0]["metadata"].pop("valid-replication-keys")

    for key in schema.properties:
        # hence, when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type:
            inclusion = "available"
            mdata.extend(
                [
                    {
                        "breadcrumb": ["properties", key, "properties", prop],
                        "metadata": {"inclusion": inclusion},
                    }
                    for prop in schema.properties.get(key).properties
                ]
            )
        else:
            inclusion = (
                "automatic"
                if key in key_properties + [replication_key]
                else "available"
            )
            mdata.append(
                {
                    "breadcrumb": ["properties", key],
                    "metadata": {"inclusion": inclusion},
                }
            )

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(
            stream_id, schema, get_key_properties(stream_id)
        )
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
            )
        )
    return Catalog(streams)


def daterange(date1, date2):
    for n in range(int((date2 - date1).days)+1):
        yield date1 + timedelta(n)

def generate_dates_to_today(date_from_str:str, config):
    format = '%Y-%m-%d'
    date_from = datetime.datetime.strptime(date_from_str, format)-timedelta(days=int(config["date_offset_days"]))
    date_to = datetime.datetime.today()

    for dt in daterange(date_from, date_to):
        yield dt.strftime(format)


def sync(config, state, catalog):
    # backup state
    singer.write_state(state)

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        bookmark_column = get_bookmark(stream.tap_stream_id)
        bookmark_value = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column, 1 if bookmark_column == 'page' else config["date_from"])

        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        tapfiliate_client = TapfiliateRestApi(x_api_key=config["x-api-token"], retry=5)
        if stream.tap_stream_id not in TapfiliateRestApi.tapfiliate_get_streams:
            raise Exception(f"Unknown stream : {stream.tap_stream_id}")

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            if bookmark_column == 'page':
                if int(bookmark_value)>10:
                    bookmark_value=str(int(int(bookmark_value) * int(config["page_offset_percentage"])/100)) #offsetting two percent of pages from the last bookmarked page
                for page, record in tapfiliate_client.get_sync_endpoints(
                    stream.tap_stream_id, parameters={bookmark_column: bookmark_value}
                ):
                    singer.write_record(stream.tap_stream_id, record)
                    counter.increment()

                    state = singer.write_bookmark(
                        state, stream.tap_stream_id, bookmark_column, page
                    )
                    singer.write_state(state)

            elif bookmark_column == 'date_from':
                for date_from in generate_dates_to_today(bookmark_value, config):
                    for _, record in tapfiliate_client.get_sync_endpoints(
                            stream.tap_stream_id, parameters={'date_from': date_from,
                                                              'date_to': date_from}
                    ):
                        singer.write_record(stream.tap_stream_id, record)
                        counter.increment()

                state = singer.write_bookmark(
                    state, stream.tap_stream_id, bookmark_column, date_from
                )
                singer.write_state(state)


    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    LOGGER.info(f"Current config args : {args}")

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
