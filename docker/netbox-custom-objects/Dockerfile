ARG NETBOX_IMAGE=netboxcommunity/netbox:v4.6.2
FROM ${NETBOX_IMAGE}

USER root
# the venv carries no pip, so the plugin is installed with the system one and
# targeted at the venv's site-packages -- asked for by name, since the base
# image's python minor version moves.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && /usr/bin/python3 -m pip install --no-cache-dir \
       --target "$(/opt/netbox/venv/bin/python -c 'import site; print(site.getsitepackages()[0])')" \
       netboxlabs-netbox-custom-objects==0.6.1 \
    && apt-get purge -y --auto-remove python3-pip \
    && rm -rf /var/lib/apt/lists/*
USER netbox
