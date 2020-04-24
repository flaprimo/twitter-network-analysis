FROM pypy:3-slim

# install system dependencies
RUN apt update && \
    apt dist-upgrade -y && \
    rm -rf /var/lib/apt/lists/* && \
    cp /usr/share/zoneinfo/Europe/Rome /etc/localtime

# set python path and working directory
WORKDIR /opt/twitter-network-analysis

# set env vars
#ENV PYTHONPATH "${PYTHONPATH}:/opt/chatbot-telegram-bot/"
#ENV IBM_CREDENTIALS_FILE="/opt/chatbot-telegram-bot/conf/ibm-credentials.env"
ENV LANG it_IT.utf8

# copy project and install requirements.txt dependencies
COPY . .
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

# run application
CMD ["pypy3", "./src/twitter-network-analysis.py"]