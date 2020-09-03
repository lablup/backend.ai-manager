import sys
from pathlib import Path


def main():
    input_path = Path(sys.argv[1])  # full path for vroot/local
    output_path = Path(sys.argv[2])  # full path for volume directory. ex. vfs/
    print(input_path, output_path)
    subfolders = [x for x in input_path.iterdir() if x.is_dir()]

    print("Number of folders ", len(subfolders))
    try:

        for folder in subfolders:
            folder = str(folder)
            folder = folder.split("/")[-1]
            header_dir = folder[0:2]
            second_lvl_dir = folder[2:4]
            rest_dir = folder[4:]

            (output_path / header_dir / second_lvl_dir / rest_dir).mkdir(parents=True, exist_ok=True)

            Path.rename(input_path / folder, output_path / header_dir / second_lvl_dir / rest_dir)

    except OSError:
        print("Creation of the directories failed")
    finally:
        print("Successfully created the directories")


if __name__ == '__main__':
    main()
    print("Done")
