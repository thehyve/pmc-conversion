import collections
import hashlib
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

checksum_algorithm = 'sha1'
checksum_file_extension = '.' + checksum_algorithm


def read_sha1_file(checksum_file) -> str:
    """ Returns the 40 hex characters sha1 from file. """
    checksum_length = 40

    with open(checksum_file, 'r') as f:
        checksum = f.read()[:checksum_length]
    return checksum


def compute_sha1(path) -> str:
    """ Generates sha1 hex digest for a file. """

    return compute_checksum(path, 'sha1')


def compute_sha256(path) -> str:
    """ Generates sha256 hex digest for a file. """

    return compute_checksum(path, 'sha256')


def compute_checksum(path, algorithm: str) -> str:
    """ Generates a hex digest using specified algorithm for a file. """
    buffer_size = 65536

    hash_builder = hashlib.new(algorithm)

    with open(path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            hash_builder.update(data)

    return hash_builder.hexdigest()


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


def calculate_file_checksum_pairs(dir):
    checksum_files = scan_files_checksums(dir)
    result = set()
    for file, checksum in checksum_files:
        result.add(DataFileChecksumPair(make_path_relative(dir, file), checksum))
    return result


def calculate_file_checksum_pairs_considering_checksum_files(dir):
    files_pairs = scan_data_checksum_files_pairs(dir)
    checksum_files = ensure_checksum_matches(files_pairs)
    files = set()
    for file, checksum in checksum_files:
        files.add(DataFileChecksumPair(make_path_relative(dir, file), checksum))
    return files
