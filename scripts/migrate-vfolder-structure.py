import sys
import os


def main():
    input_path = sys.argv[1]  # full path for vroot/local
    output_path = sys.argv[2]  # full path for volume directory. ex. vfs/
    print(input_path, output_path)

    subfolders = [f.path for f in os.scandir(input_path) if f.is_dir()]
    print("Subfolders ", subfolders)
    for folder in subfolders:

        folder = folder.split("/")[-1]
        header_dir = folder[0:2]
        second_lvl_dir = folder[2:4]
        rest_dir = folder[4:]

        list_of_files = os.listdir(input_path + "/" + folder)
        print(list_of_files)
        print("list of files ", list_of_files)
        try:
            if not os.path.isdir(output_path + "/" + header_dir):
                os.mkdir(output_path + "/" + header_dir)
            if not os.path.isdir(output_path + "/" + header_dir + "/" + second_lvl_dir):
                os.mkdir(output_path + "/" + header_dir + "/" + second_lvl_dir)
            if not os.path.isdir(output_path + "/" + header_dir + "/" + second_lvl_dir + "/" + rest_dir):
                os.mkdir(output_path + "/" + header_dir + "/" + second_lvl_dir + "/" + rest_dir)

            os.rename(input_path + "/" + folder, output_path + "/" + header_dir + "/"
                      + second_lvl_dir + "/" + rest_dir)

        except OSError:
            print("Creation of the directories failed")
        finally:
            print("Successfully created the directories")


if __name__ == '__main__':
    main()
    print("Done")
