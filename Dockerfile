FROM apache/airflow:2.7.2

USER root

# Cài Chrome, ChromeDriver và Docker CLI
RUN apt-get update && apt-get install -y \
    wget curl unzip gnupg lsb-release ca-certificates \
    netcat-traditional xvfb fonts-liberation \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 \
    libcups2 libdbus-1-3 libdrm2 libgbm1 libgtk-3-0 \
    libnspr4 libnss3 libxcomposite1 libxdamage1 libxfixes3 \
    libxkbcommon0 libxrandr2 xdg-utils libu2f-udev libvulkan1 \
    apt-transport-https \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && curl -sSL https://chromedriver.storage.googleapis.com/LATEST_RELEASE | xargs -I {} wget -q -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/{}/chromedriver_linux64.zip \
    && unzip /tmp/chromedriver.zip -d /usr/local/bin/ && chmod +x /usr/local/bin/chromedriver \
    && rm /tmp/chromedriver.zip \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && echo "deb [arch=$(dpkg --print-architecture)] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update && apt-get install -y docker-ce-cli \
    && groupadd -f docker && usermod -aG docker airflow \
    && apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

# Cài Python packages
RUN pip install --no-cache-dir \
    selenium==4.9.1 \
    beautifulsoup4==4.12.2 \
    fake_useragent==1.1.3 \
    webdriver-manager==3.8.6

# Thiết lập thư mục làm việc
WORKDIR ${AIRFLOW_HOME}

# Kiểm tra Airflow đã được cài
RUN python -c "import airflow; print(f'✔️ Found Airflow version: {airflow.__version__}')"
