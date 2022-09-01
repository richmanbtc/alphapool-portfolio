import math
import os
import time
import pandas as pd
import dataset
from retry import retry
from alphapool import Client
from .market_data_store.data_fetcher_builder import DataFetcherBuilder
from .market_data_store.market_data_store import MarketDataStore
from .logger import create_logger
from .portfolios.equal_weight import EqualWeight
from .portfolios.universal import Universal
from .processing import preprocess_df, calc_model_ret

log_level = os.getenv("ALPHAPOOL_LOG_LEVEL")
logger = create_logger(log_level)


@retry(tries=3, delay=3)
def job(dry=False):
    optimization_days = int(os.getenv("ALPHAPOOL_OPTIMIZATION_DAYS"))
    model_id = os.getenv("ALPHAPOOL_MODEL_ID")
    interval = 5 * 60
    tournament = "crypto"

    logger.info("job started")
    execution_time = math.floor(time.time() / interval) * interval
    execution_time = pd.to_datetime(execution_time, unit="s", utc=True)
    logger.info("execution_time {}".format(execution_time))

    data_fetcher_builder = DataFetcherBuilder()
    market_data_store = MarketDataStore(
        data_fetcher_builder=data_fetcher_builder,
        start_time=time.time() - 24 * 60 * 60 * optimization_days * 2,
        logger=logger,
        interval=5 * 60,
    )

    database_url = os.getenv("ALPHAPOOL_DATABASE_URL")
    db = dataset.connect(database_url)
    client = Client(db)

    df = client.get_positions(tournament="crypto")
    df = preprocess_df(df, execution_time)
    logger.debug(df)

    symbols = df.columns.str.replace("p.", "", regex=False).to_list()
    logger.debug(symbols)

    df_ret = market_data_store.fetch_df_market(symbols=symbols)
    df = df.join(df_ret).dropna()
    logger.debug(df_ret)

    df_model_ret = calc_model_ret(df).dropna()
    logger.debug(df_model_ret)

    # portfolio = EqualWeight()
    portfolio = Universal()
    df_weights = portfolio.get_weights(df_model_ret)
    logger.debug(df_weights)

    if not dry:
        client.submit(
            tournament=tournament,
            model_id=model_id,
            timestamp=int(execution_time.timestamp()),
            weights=df_weights['weight'].to_dict(),
        )
    logger.info("job finished")


job()
