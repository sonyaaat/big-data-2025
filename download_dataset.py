import os
import urllib.request
import gzip
import shutil


data_dir = "imdb_data"
os.makedirs(data_dir, exist_ok=True)

files = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]

base_url = "https://datasets.imdbws.com/"

print("Downloading IMDB datasets...")

for file in files:
    url = base_url + file
    file_path = os.path.join(data_dir, file)
    print(f"Downloading {file}...")
    urllib.request.urlretrieve(url, file_path)

print("All downloads complete! Extracting files...")

for file in files:
    file_path = os.path.join(data_dir, file)
    extracted_path = file_path.replace(".gz", "")
    print(f"Extracting {file}...")
    with gzip.open(file_path, 'rb') as f_in, open(extracted_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)

print("Extraction complete!")
print("Downloaded and extracted files:")
print(os.listdir(data_dir))
