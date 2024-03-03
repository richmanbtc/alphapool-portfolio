import math
import json
import os
import time
import numpy as np
import pandas as pd
import dataset
from retry import retry
from alphapool import Client
from .logger import create_logger
from .processing import remove_portfolio_models, remove_inactive_models

log_level = os.getenv("ALPHAPOOL_LOG_LEVEL")
logger = create_logger(log_level)


@retry(tries=3, delay=3, logger=logger)
def job(dry=False):
    logger.info("job started")

    portfolio_type = os.getenv("ALPHAPOOL_PORTFOLIO")
    max_leverage = float(os.getenv("ALPHAPOOL_MAX_LEVERAGE"))
    model_id = os.getenv("ALPHAPOOL_MODEL_ID")
    exchange = os.getenv("ALPHAPOOL_EXCHANGE")
    model_id_regex = os.getenv("ALPHAPOOL_MODEL_ID_REGEX", '.*')
    symbol_whitelist = json.loads(os.getenv("ALPHAPOOL_SYMBOLS", '[]'))
    interval = 5 * 60

    logger.info('portfolio_type {}'.format(portfolio_type))
    logger.info('max_leverage {}'.format(max_leverage))
    logger.info('model_id {}'.format(model_id))
    logger.info('exchange {}'.format(exchange))
    logger.info('model_id_regex {}'.format(model_id_regex))
    logger.info('symbol_whitelist {}'.format(symbol_whitelist))
    logger.info('interval {}'.format(interval))

    execution_time = math.ceil(time.time() / interval) * interval
    execution_time = pd.to_datetime(execution_time, unit="s", utc=True)
    logger.info("execution_time {}".format(execution_time))

    database_url = os.getenv("ALPHAPOOL_DATABASE_URL")
    db = dataset.connect(database_url)
    client = Client(db)

    df = client.get_positions((execution_time - pd.to_timedelta(1, unit="D")).timestamp())
    df = df.loc[df.index.get_level_values('model_id').str.fullmatch(model_id_regex)]
    logger.debug('raw')
    logger.debug(df)

    df = remove_portfolio_models(df)
    if len(symbol_whitelist) > 0:
        df = remove_out_of_universe_models(df, execution_time - pd.to_timedelta(1, unit="D"), symbols=symbol_whitelist)
        logger.info('out of universe model removed')
    df = select_exchange_models(df, exchange)
    logger.debug('preprocessed')
    logger.debug(df)

    if portfolio_type != 'equal_weight':
        raise Exception('unknown portfolio {}'.format(portfolio_type))

    model_ids = df.index.get_level_values('model_id').unique()
    df_weights = pd.DataFrame(
        np.ones((model_ids.size, 1)) / (1e-37 + model_ids.size) * max_leverage,
        index=model_ids,
        columns=["weight"],
    )
    logger.debug(df_weights)

    if not dry:
        client.submit(
            model_id=model_id,
            timestamp=int(execution_time.timestamp()),
            weights=df_weights['weight'].to_dict(),
        )
    logger.info("job finished")


def remove_out_of_universe_models(df, min_timestamp, symbols):
    df_out = df.loc[df.index.get_level_values('timestamp') >= min_timestamp]
    excluded_model_ids = []
    for index, row in df_out.iterrows():
        model_id = index[df_out.index.names.index('model_id')]
        for symbol in list(row['positions'].keys()) + list(row['orders'].keys()):
            if symbol not in symbols:
                excluded_model_ids.append(model_id)
    return df.loc[~df.index.get_level_values("model_id").isin(excluded_model_ids)]


def select_exchange_models(df, exchange):
    df_exchange = df.groupby('model_id')['exchange'].nth(-1)
    df_exchange = df_exchange.loc[(df_exchange == exchange) | df_exchange.isna()]
    return df.loc[df.index.get_level_values("model_id").isin(df_exchange.index)]


logger.info('''simplified version of main script
- equal weight
- not depend on market data
- work with alphapool >=v0.1.1''')

job()
