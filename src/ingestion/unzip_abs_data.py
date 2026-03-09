from pathlib import Path
import zipfile

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"


def unzip_abs_zip():
    zip_files = list(RAW_DIR.glob("*.zip"))

    if not zip_files:
        print("No zip file found in data/raw")
        return

    zip_path = zip_files[0]
    extract_dir = RAW_DIR / "abs_unzipped"
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    print(f"Unzipped: {zip_path.name}")
    print(f"Files extracted to: {extract_dir}")


if __name__ == "__main__":
    unzip_abs_zip()
