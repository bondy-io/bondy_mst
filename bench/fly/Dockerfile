# =============================================================================
# bondy_mst — Linux bench substrate for Fly.io
#
# Long-lived SSH-only Debian 12 image with:
#   - kerl-built OTP 28
#   - Elixir 1.18 (compatible with OTP 28)
#   - rebar3 + mix + just
#   - sysstat (iostat, vmstat), strace, procps — the companion-data
#     tools the bench plan §4 calls for
#   - The bondy_mst repo pre-compiled under both rebar3 and mix so
#     the first ssh session doesn't pay compile cost
#
# See `bench/fly/README.md` for the operator runbook and
# `_design/latest/PACK_STORE_WRITE_PATH_FLOOR_BENCH_PLAN.md` §10 for
# the QA #14 execution context.
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1 — OTP build
#
# The kerl build is the slowest part of the image (~5-10 min cold).
# We bake it into a dedicated stage so changes to project source
# don't bust the OTP layer cache.
# -----------------------------------------------------------------------------
FROM debian:12-slim AS otp-builder

ARG OTP_VERSION=28.0
ARG KERL_VERSION=4.4.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        autoconf \
        m4 \
        libncurses5-dev \
        libssl-dev \
        libodbc1 \
        unixodbc-dev \
        curl \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/local/bin/kerl \
        https://raw.githubusercontent.com/kerl/kerl/${KERL_VERSION}/kerl \
    && chmod +x /usr/local/bin/kerl

# kerl options pinned for a minimal OTP build matching production:
# HiPE off (deprecated), wx/odbc off (not used), docs off (not needed
# in the bench substrate).
ENV KERL_CONFIGURE_OPTIONS="--disable-hipe --without-wx --without-odbc --without-debugger --without-observer --without-et --disable-silent-rules"

RUN kerl build ${OTP_VERSION} ${OTP_VERSION} \
    && kerl install ${OTP_VERSION} /opt/otp/${OTP_VERSION} \
    && kerl cleanup ${OTP_VERSION}

# -----------------------------------------------------------------------------
# Stage 2 — runtime image
# -----------------------------------------------------------------------------
FROM debian:12-slim

ARG OTP_VERSION=28.0
ARG ELIXIR_VERSION=1.18.4
ARG REBAR3_VERSION=3.24.0
ARG JUST_VERSION=1.36.0

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PATH=/opt/otp/${OTP_VERSION}/bin:/opt/elixir/bin:/usr/local/bin:/usr/bin:/bin

# Runtime + bench companion tools. The bench plan §4 calls for
# iostat (sysstat), vmstat (procps), strace explicitly.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        libncurses6 \
        libssl3 \
        libodbc1 \
        procps \
        strace \
        sysstat \
        less \
        vim-tiny \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# Pull the OTP install from stage 1.
COPY --from=otp-builder /opt/otp /opt/otp

# Elixir — match OTP 28. Elixir 1.18+ added OTP 28 support.
RUN mkdir -p /opt/elixir \
    && curl -fsSL -o /tmp/elixir.zip \
        https://github.com/elixir-lang/elixir/releases/download/v${ELIXIR_VERSION}/elixir-otp-28.zip \
    && unzip -q /tmp/elixir.zip -d /opt/elixir \
    && rm /tmp/elixir.zip

# rebar3 binary (pinned).
RUN curl -fsSL -o /usr/local/bin/rebar3 \
        https://github.com/erlang/rebar3/releases/download/${REBAR3_VERSION}/rebar3 \
    && chmod +x /usr/local/bin/rebar3

# just (justfile runner). The project's bench commands are driven
# through `just bench-*` recipes so the image needs it on PATH.
RUN curl -fsSL -o /tmp/just.tar.gz \
        "https://github.com/casey/just/releases/download/${JUST_VERSION}/just-${JUST_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
    && tar -xzf /tmp/just.tar.gz -C /usr/local/bin just \
    && rm /tmp/just.tar.gz

# Mix + hex + rebar3 archive bootstrap. Done once at build time
# (not at runtime) so first ssh into a fresh machine doesn't fetch.
RUN mix local.hex --force \
    && mix local.rebar --force

# Project source. Copied last so source edits only invalidate the
# tail of the layer cache. `_build` is excluded via .dockerignore;
# the runtime symlink to /data/_build keeps it on the volume.
WORKDIR /opt/bondy_mst
COPY . /opt/bondy_mst

# Pre-fetch Erlang and Elixir deps. Pre-compile rebar3 default
# profile (production build) and the bench Mix project so first
# ssh starts at usable speed.
RUN rebar3 compile \
    && cd bench && mix deps.get && mix compile

# Entrypoint:
#   1. Symlink /tmp and /opt/bondy_mst/_build to the persistent
#      Fly volume so artefacts survive machine restarts.
#   2. Stay alive (Fly machines stop when PID 1 exits).
COPY bench/fly/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["tail", "-f", "/dev/null"]
