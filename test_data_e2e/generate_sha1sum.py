import hashlib
import os


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


def traverse(top_dir, algorithm: str = 'sha1'):
    for root, d_names, f_names in os.walk(top_dir):
        for f_name in f_names:
            if f_name.endswith(algorithm):
                continue
            f_in = os.path.join(root, f_name)
            f_out = f_in + '.' + algorithm
            checksum = compute_checksum(f_in, algorithm)
            with open(f_out, 'w') as f:
                f.write(checksum + '  ' + f_name)


if __name__ == '__main__':
    traverse('test_data_e2e')
