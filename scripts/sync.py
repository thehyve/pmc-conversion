import collections
import logging
import os
import sys
from shutil import copyfile

from .checksum import read_sha1_file, compute_sha1

logger = logging.getLogger(__name__)

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
            logger.debug('Skip {}'.format(data_file))
            continue
        checksum_file = get_checksum_file(data_file)
        if checksum_file in checksum_files:
            checksum_files.remove(checksum_file)
            yield DataChecksumFilesPair(data_file, checksum_file)
        else:
            # TODO improve logging for the pipeline
            emsg = 'The data file {} does not have corresponding checksum file.'.format(data_file)
            logger.error(emsg)
            raise FileNotFoundError(emsg)
    if checksum_files:
        checksum_files_csv = ', '.join(checksum_files)
        emsg = 'The following checksum files does not have corresponding data file: {}'.format(checksum_files_csv)
        logger.error(emsg)
        raise FileNotFoundError(emsg)


DataFileChecksumPair = collections.namedtuple('DataFileChecksumPair', ['data_file', 'checksum'])


def ensure_checksum_matches(data_checksum_files_pairs):
    for pair in data_checksum_files_pairs:
        computed_checksum = compute_sha1(pair.data_file)
        if computed_checksum == read_sha1_file(pair.checksum_file):
            yield DataFileChecksumPair(pair.data_file, computed_checksum)
        else:
            emsg = 'Checksum for {} file does not match.'.format(pair.data_file)
            logger.error(emsg)
            raise ValueError(emsg)


def scan_files_checksums(dir_):
    """
    Yield all data files with their checksums based on file content.
    """
    if not os.path.exists(dir_):
        emsg = '{} is not a directory.'.format(dir_)
        logger.error(emsg)
        raise NotADirectoryError(emsg)
    for root, _, files in os.walk(dir_):
        for file in files:
            if not is_checksum_file(file) and not is_hiden_file(file):
                path = os.path.join(root, file)
                yield DataFileChecksumPair(path, compute_sha1(path))


def scan_data_checksum_files_pairs(dir_):
    pairs = []
    for root, _, files in os.walk(dir_):
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
        emsg = '{} is not sub directory of the {}.'.format(child_path, parent_path)
        logger.error(emsg)
        raise ValueError(emsg)


def get_checksum_pairs_set(dir_):
    checksum_files = scan_files_checksums(dir_)
    result = set()
    for file, checksum in checksum_files:
        result.add(DataFileChecksumPair(make_path_relative(dir_, file), checksum))
    return result


def is_dirs_in_sync(dir1, dir2):
    return get_checksum_pairs_set(dir1) == get_checksum_pairs_set(dir2)


class FilesModifications:
    new_files = None
    old_files = None

    @property
    def add_files(self):
        return self.new_files - self.old_files

    @property
    def remove_files(self):
        return self.old_files - self.new_files

    @property
    def no_changes(self):
        return self.old_files == self.new_files


def sync_dirs(from_dir, to_dir):
    logger.info('Check source ({}) directory consistency...'.format(from_dir))
    files_pairs = scan_data_checksum_files_pairs(from_dir)
    from_checksum_files = ensure_checksum_matches(files_pairs)
    new_files = set()
    for file, checksum in from_checksum_files:
        new_files.add(DataFileChecksumPair(make_path_relative(from_dir, file), checksum))

    file_modifications = FilesModifications()
    file_modifications.new_files = new_files
    logger.info('Reading destination ({}) directory content to compare...'.format(to_dir))
    file_modifications.old_files = get_checksum_pairs_set(to_dir)
    if file_modifications.no_changes:
        logger.info('No changes detected. Exit.')
        return file_modifications

    logger.info('Differences detected. Start synchronising...')

    remove_files = file_modifications.remove_files
    logger.info('Start removing {} files from the destination directory...'.format(len(remove_files)))
    for remove_file in remove_files:
        remove_path = os.path.join(to_dir, remove_file.data_file)
        logger.debug('Removing {} file.'.format(remove_path))
        os.remove(remove_path)

    add_files = file_modifications.add_files
    logger.info('Start copying {} files from the source to destination directory...'.format(len(add_files)))
    for add_file in add_files:
        src_path = os.path.join(from_dir, add_file.data_file)
        dst_path = os.path.join(to_dir, add_file.data_file)
        logger.debug('Copying {} file to {}.'.format(src_path, dst_path))
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        copyfile(src_path, dst_path)

    logger.info('Double checking whether we copied exactly the same content as we expected.')
    if not file_modifications.new_files == get_checksum_pairs_set(to_dir):
        emsg = 'It seems like source directory files have changed while their copying.'
        logger.error(emsg)
        raise ValueError(emsg)

    return file_modifications


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('sync <from directory> <to directory>')
        sys.exit(1)
    from_directory = sys.argv[1]
    to_directory = sys.argv[2]
    sync_dirs(from_directory, to_directory)
    sys.exit(0)
