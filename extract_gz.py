
import gzip, shutil
from pathlib import Path

directory = r"C:\Users\MatthewHile\Desktop\Labs\imdb_to_azure\gz_files"

def extract_all_in_directory(directory):

    directory = Path(directory)
    
    if not directory.exists():
        raise FileNotFoundError(f"{directory} does not exist")
    
    gz_files = list(directory.glob("*.gz"))
    
    for gz_file in gz_files:
        try:
            extract_gz(gz_file)
        except Exception as e:
            print(f"Failed to extract {gz_file.name}: {e}")


def extract_gz(file_path):

    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"{file_path} does not exist")

    if file_path.suffix != ".gz":
        raise ValueError("File must have .gz extension")
    
    # Remove .gz extension to create output filename
    tsv_file = file_path.with_suffix("")  
    
    with gzip.open(file_path, "rb") as f_in:
        with open(tsv_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    print(f"Extracted {file_path.name} -> {tsv_file.name}")


extract_all_in_directory(directory)