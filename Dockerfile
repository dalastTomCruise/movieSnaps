FROM public.ecr.aws/lambda/python:3.13

# Install dependencies
COPY pyproject.toml poetry.lock ./
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main --no-interaction --no-ansi --no-root

# Copy source
COPY config.py scraper.py storage.py agent.py pipeline.py lambda_handler.py ./

CMD ["lambda_handler.handler"]
