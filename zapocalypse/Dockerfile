FROM python:3.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.11.4 /uv /uvx /bin/

ADD . /zapocalypse

WORKDIR /zapocalypse

RUN uv sync --locked

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]