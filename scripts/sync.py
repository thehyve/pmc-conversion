import collections
import logging
import os
import sys
from shutil import copyfile

from checksum import read_sha1_file, compute_sha1

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

checksum_algorithm = 'sha1'
checksum_file_extension = '.' + checksum_algorithm


def is_checksum_file(file):
    return file.endswith(checksum_file_extension)


def get_checksum_file(data_file):
    return data_file + checksum_file_extension


def split_data_and_checksum_files(files: list):
    checksum_files = []
    data_files = []
    for file in files:
        if is_checksum_file(file):
            checksum_files.append(file)
        else:
            data_files.append(file)
    return data_files, checksum_files


DataChecksumFilesPair = collections.namedtuple('DataChecksumFilesPair', ['data_file', 'checksum_file'])

def is_hiden_file(path):
    return os.path.basename(path).startswith('.')

def get_data_checksum_file_pairs(files):
    data_files, checksum_files = split_data_and_checksum_files(files)
    for data_file in data_files:
        if is_hiden_file(data_file):
            logger.debug(f'Skip {data_file}')
            continue
        checksum_file = get_checksum_file(data_file)
        if checksum_file in checksum_files:
            checksum_files.remove(checksum_file)
            yield DataChecksumFilesPair(data_file, checksum_file)
        else:
            raise FileNotFoundError(f'The data file {data_file} does not have corresponding checksum file.')
    if checksum_files:
        checksum_files_csv = ', '.join(checksum_files)
        raise FileNotFoundError('The following checksum files does not have corresponding data file: '
                                + checksum_files_csv)


DataFileChecksumPair = collections.namedtuple('DataFileChecksumPair', ['data_file', 'checksum'])


def ensure_checksum_matches(data_checksum_files_pairs):
    for pair in data_checksum_files_pairs:
        computed_checksum = compute_sha1(pair.data_file)
        if computed_checksum == read_sha1_file(pair.checksum_file):
            yield DataFileChecksumPair(pair.data_file, computed_checksum)
        else:
            raise ValueError(f'Checksum for {pair.data_file} file does not match.')


def scan_files_checksums(dir):
    if not os.path.exists(dir):
        raise NotADirectoryError(f'{dir} is not a directory.')
    for root, _, files in os.walk(dir):
        for file in files:
            if not is_checksum_file(file) and not is_hiden_file(file):
                path = os.path.join(root, file)
                yield DataFileChecksumPair(path, compute_sha1(path))


def scan_data_checksum_files_pairs(dir):
    pairs = []
    for root, _, files in os.walk(dir):
        paths = [os.path.join(root, file) for file in files]
        for pair in get_data_checksum_file_pairs(paths):
            pairs.append(pair)
    return pairs


def make_path_relative(parent_path, child_path):
    if child_path.startswith(parent_path):
        result = child_path.replace(parent_path, '')
        if result.startswith('/'):
            result = result[1:]
        return result
    else:
        raise ValueError(f'{child_path} is not sub directory of the {parent_path}.')


def get_checksum_pairs_set(dir):
    checksum_files = scan_files_checksums(dir)
    result = set()
    for file, checksum in checksum_files:
        result.add(DataFileChecksumPair(make_path_relative(dir, file), checksum))
    return result


def is_dirs_in_sync(dir1, dir2):
    return get_checksum_pairs_set(dir1) == get_checksum_pairs_set(dir2)


FilesModifications = collections.namedtuple('FilesModifications', ['added', 'removed'])


def sync_dirs(from_dir, to_dir):
    logger.info(f'Check source ({from_dir}) directory consistency...')
    files_pairs = scan_data_checksum_files_pairs(from_dir)
    from_checksum_files = ensure_checksum_matches(files_pairs)
    from_dir_files = set()
    for file, checksum in from_checksum_files:
        from_dir_files.add(DataFileChecksumPair(make_path_relative(from_dir, file), checksum))

    logger.info(f'Reading destination ({to_dir}) directory content to compare...')
    to_dir_files = get_checksum_pairs_set(to_dir)

    if from_dir_files == to_dir_files:
        logger.info('No changes detected. Exit.')
        return FilesModifications(set(), set())

    logger.info('Differences detected. Start synchronising...')
    remove_files = to_dir_files - from_dir_files
    add_files = from_dir_files - to_dir_files

    logger.info(f'Start removing {len(remove_files)} files from the destination directory...')
    for remove_file in remove_files:
        remove_path = os.path.join(to_dir, remove_file.data_file)
        logger.debug(f'Removing {remove_path} file.')
        os.remove(remove_path)

    logger.info(f'Start copying {len(add_files)} files from the source to destination directory...')
    for add_file in add_files:
        src_path = os.path.join(from_dir, add_file.data_file)
        dst_path = os.path.join(to_dir, add_file.data_file)
        logger.debug(f'Copying {src_path} file to {dst_path}.')
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        copyfile(src_path, dst_path)

    return FilesModifications(add_files, remove_files)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('sync <from directory> <to directory>')
        sys.exit(1)
    from_directory = sys.argv[1]
    to_directory = sys.argv[2]
    sync_dirs(from_directory, to_directory)
    sys.exit(0)
