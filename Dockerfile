FROM unit:python3.12

# copy python requirements to image
COPY requirements.txt /config/requirements.txt

# install python requirments
RUN python3 -m pip install -r /config/requirements.txt

# To run code with bind commands comment out below lines (see readme)
COPY ./api /api/
# COPY ./api /www/api

COPY ./css /www/css
COPY ./js /www/js
COPY ./agrid_index.html /www/index.html

# docker uses config.json to setup unit
COPY ./config/config.json /docker-entrypoint.d/