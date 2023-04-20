FROM apache/airflow:latest

# Install Chrome Webdriver
USER root
RUN apt-get update && apt-get install -y wget gnupg curl unzip
RUN apt-get update && apt-get install -y build-essential
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
RUN apt-get update && apt-get install -y google-chrome-stable
RUN CHROMEDRIVER_VERSION=$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE) && \
    wget -N http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip -P ~/ && \
    unzip ~/chromedriver_linux64.zip -d ~/ && \
    rm ~/chromedriver_linux64.zip && \
    mv -f ~/chromedriver /usr/local/bin/chromedriver && \
    chown root:root /usr/local/bin/chromedriver && \
    chmod 0755 /usr/local/bin/chromedriver
RUN apt-get update && apt-get install -y libxss1 libasound2 libappindicator3-1 libatk-bridge2.0-0 libgtk-3-0 fonts-liberation xdg-utils
USER airflow
# Install bertopic
RUN pip install hdbscan
RUN pip install bertopic
# Install other packages
RUN pip install bs4
RUN pip install selenium
RUN pip install mysql
RUN pip install requests
RUN pip install pandas
RUN pip install spacy
RUN pip install nltk
RUN pip install spacytextblob
RUN python -m spacy download en_core_web_sm