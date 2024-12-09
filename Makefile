setup_producer:
	cd app_producer_taxitrips && \
	python3.12 -m venv .venv_app_producer && \
	source .venv_app_producer/bin/activate && \
	pip install --upgrade pip && \
	pip install -r requirements.txt
	
setup_consumer:
	cd app_consumer_taxitrips && \
	python3.12 -m venv .venv_app_consumer && \
	source .venv_app_consumer/bin/activate && \
	pip install --upgrade pip && \
	pip install -r requirements.txt