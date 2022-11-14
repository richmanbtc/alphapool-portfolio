FROM python:3.10.6

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    ccxt==1.57.43 \
    "git+https://github.com/richmanbtc/ccxt_rate_limiter.git@v0.0.6#egg=ccxt_rate_limiter" \
    "git+https://github.com/richmanbtc/crypto_data_fetcher.git@v0.0.18#egg=crypto_data_fetcher" \
    cvxpy==1.2.1 \
    schedule==1.1.0 \
    "git+https://github.com/richmanbtc/alphapool.git@v0.0.8#egg=alphapool" \
    dataset==1.5.2 \
    psycopg2==2.9.3 \
    retry==0.9.2 \
    universal-portfolios

ADD . /app
ENV ALPHAPOOL_LOG_LEVEL=debug \
    ALPHAPOOL_EXECUTION_COST=0.001 \
    ALPHAPOOL_OPTIMIZATION_DAYS=60 \
    ALPHAPOOL_MAX_LEVERAGE=5 \
    ALPHAPOOL_SCORE_THRESHOLD=0.5 \
    ALPHAPOOL_INTERVAL=300 \
    ALPHAPOOL_PORTFOLIO=universal \
    ALPHAPOOL_MODEL_ID=pf-universal
WORKDIR /app
CMD python -m src.main
