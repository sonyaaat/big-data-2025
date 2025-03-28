#!/bin/bash
python download_dataset.py
python schemas.py
python imdb_spark_utils.py
python main.py