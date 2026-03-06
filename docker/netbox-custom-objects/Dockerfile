ARG NETBOX_IMAGE=netboxcommunity/netbox:latest
FROM ${NETBOX_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && /usr/bin/python3 -m pip install --no-cache-dir --target /opt/netbox/venv/lib/python3.12/site-packages netboxlabs-netbox-custom-objects==0.4.6 \
    && apt-get purge -y --auto-remove python3-pip \
    && rm -rf /var/lib/apt/lists/*
USER netbox
