import os

from scripts.checksum import compute_checksum


def compute_checksum_in_folder(top_dir, algorithm: str):
    """Traverse folder and write checksum file for each file found."""
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
    compute_checksum_in_folder('test_data_e2e/current/dropzone', 'sha1')
