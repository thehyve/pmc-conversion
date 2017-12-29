import hashlib


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
