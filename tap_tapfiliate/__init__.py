#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_tapfiliate.tapfiliate_client import TapfiliateRestApi

REQUIRED_CONFIG_KEYS = ["x-api-token"]

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
        # "surveys_responses": "recorded_date"
    }
    return bookmark.get(stream_id)


def get_key_properties(stream_id):
    key_properties = {
        "affiliates": ["id"],
        # "surveys_groups": ["id"],
        # "surveys_questions": ["survey_id", "question_id"],
        # "surveys_responses": ["survey_id", "response_id"]
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


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        tapfiliate_client = TapfiliateRestApi(x_api_key=config["x-api-token"], retry=30)
        if stream.tap_stream_id not in TapfiliateRestApi.tapfiliate_streams:
            raise Exception(f"Unknown stream : {stream.tap_stream_id}")

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        for record in tapfiliate_client.sync_endpoints(stream.tap_stream_id):
            singer.write_record(
                stream.tap_stream_id, record
            )

    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

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
