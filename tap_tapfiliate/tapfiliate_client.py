import json
import time
import urllib

import requests
import singer

LOGGER = singer.get_logger()


class TapfiliateRestApi(object):
    tapfiliate_streams = [
        "affiliate-groups",
        "affiliate-prospects",
        "affiliates",
        "balances",
        "commissions",
        "conversions",
        "customers",
        "payments",
        "programs",
    ]

    def __init__(
        self,
        x_api_key,
        api_base="https://api.tapfiliate.com",
        api_version="1.6",
        retry=10,
    ):
        self.x_api_key = x_api_key
        self.api_base = api_base
        self.api_version = api_version
        self.retry = retry

    def sync_endpoints(self, stream, parameters={}):
        # Endpoints documentations
        # https://tapfiliate.com/docs/rest/#customers-customers-collection-get
        # https://tapfiliate.com/docs/rest/#conversions-conversions-collection-get
        # https://tapfiliate.com/docs/rest/#commissions-commissions-collection-get
        # https://tapfiliate.com/docs/rest/#affiliates-affiliates-collection-get
        # https://tapfiliate.com/docs/rest/#affiliate-groups-affiliate-group-get
        # https://tapfiliate.com/docs/rest/#affiliate-prospects-affiliate-prospects-collection-get
        # https://tapfiliate.com/docs/rest/#programs-programs-collection-get
        # https://tapfiliate.com/docs/rest/#payments-balances-collection-get
        # https://tapfiliate.com/docs/rest/#payments-payments-collection-get

        # Configure call header
        headers = {"content-type": "application/json", "X-Api-Key": self.x_api_key}

        # Set default url parameter
        if "page" not in parameters:
            parameters["page"] = 1

        more_pages = True
        current_retry = 0
        while more_pages:
            url = f"{self.api_base}/{self.api_version}/{stream}/?{urllib.parse.unquote(urllib.parse.urlencode(parameters))}"
            if parameters["page"] == 1:
                LOGGER.info(f"First get from URL : {url}")
            
            LOGGER.debug(f"Get from URL : {url}")

            try:
                response = requests.get(url, headers=headers, timeout=60)

                if response.status_code != 200:
                    if current_retry < self.retry:
                        LOGGER.warning(
                            f"Unexpected response status_code {response.status_code} i need to sleep 60s before retry {current_retry}/{self.retry}"
                        )
                        time.sleep(60)
                        current_retry = current_retry + 1
                    else:
                        raise RuntimeError(
                            f"Too many retry, last response status_code {response.status_code} : {response.content}"
                        )
                else:
                    if parameters["page"] == 1 and "link" in response.headers:
                        # debug for first run, display all links
                        LOGGER.debug(f"links : {response.headers['link']}")

                    records = json.loads(response.content.decode("utf-8"))
                    if isinstance(records, dict):
                        LOGGER.info("Last call returned one document")
                        records = [records]

                    for record in records:
                        yield parameters["page"], record

                    if len(records) <= 1:
                        LOGGER.info(
                            "No need to more calls"
                        )
                        more_pages = False
                    else:
                        parameters["page"] = parameters["page"] + 1

                        # Display next page number every xx pages
                        if parameters["page"] % 10 == 0:
                            LOGGER.info(f"Next {stream} page to get {parameters['page']}. Links : {response.headers['Link']}")

                        # The number of requests you have left before exceeding the rate limit
                        x_ratelimit_remaining = int(
                            response.headers["X-Ratelimit-Remaining"]
                        )

                        # When your number of requests will reset (Unix Timestamp in seconds)
                        x_ratelimit_reset = int(response.headers["X-Ratelimit-Reset"])

                        # if we cannot make more call : we wait until the reset
                        if x_ratelimit_remaining < 15:
                            sleep_duration = x_ratelimit_reset - time.time()
                            if sleep_duration < 30:
                                sleep_duration = 30
                            LOGGER.warning(
                                f"Remaining {x_ratelimit_remaining} call, I prefer to sleep {sleep_duration} seconds until the rest."
                            )
                            time.sleep(sleep_duration)

            except Exception as e:
                if current_retry < self.retry:
                    LOGGER.warning(
                        f"I need to sleep 60 s, Because last get call to {url} raised exception : {e}"
                    )
                    time.sleep(60)
                    current_retry = current_retry + 1
                else:
                    raise e
