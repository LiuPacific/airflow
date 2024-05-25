import re
import os


def convert_to_valid_filename(filename):
    # Replace any character that is not alphanumeric, hyphen, or period with an underscore
    # path_safe_run_id = run_id.replace(':', '_').replace(' ', '_').replace('+', '_')
    valid_filename = re.sub(r'[^a-zA-Z0-9\.\-]', '_', filename)
    return valid_filename


def directory_tree(path, level=0, base_path = "/home/typingliu/temp") -> list:
    '''
    {
        "manual__2024-05-23T21_50_53.450227_00_0013iwpubw": {},
        "manual__2024-05-23T22_44_13.809552_00_00cyxqnepw": {},
        "manual__2024-05-23T22_44_13.809552_00_00": {
            "tmp_outdir_wb0z6h5": {},
            "tmp_outdirtw6fzy5p": {},
            "tmp_outdir": {
                "docker_extraction_0": {
                    "saz5-9hgg.csv": "/manual__2024-05-23T22_44_13.809552_00_00/tmp_outdir/docker_extraction_0/saz5-9hgg.csv"
                }
            },
            "outdir": {
                "error.log": "/manual__2024-05-23T22_44_13.809552_00_00/outdir/error.log",
                "stdout.log": "/manual__2024-05-23T22_44_13.809552_00_00/outdir/stdout.log"
            },
            "hara_kv_db.json": "/manual__2024-05-23T22_44_13.809552_00_00/hara_kv_db.json"
        },
    :param path:
    :param level:
    :return:
    '''
    # Print the name of the current directory
    print("  " * level + "|-- " + os.path.basename(path))

    tree_dict = {}
    # List all files and directories in the current directory
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_dir():
                # Recursively print the tree for subdirectories
                sub_tree_dict = directory_tree(entry.path, level + 1)
                tree_dict[entry.name] = sub_tree_dict
            else:
                tree_dict[entry.name] = path[len(base_path):]+"/"+entry.name
                # Print the file name
                print("  " * (level + 1) + "|-- " + entry.name)

    return tree_dict
