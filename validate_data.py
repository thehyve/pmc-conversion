import os
import shutil
import subprocess


config_dir = 'config'
output_folder = 'validation_results'
out_csr = os.path.join(output_folder, 'sources2csr')
out_tm = os.path.join(output_folder, 'csr2transmart')


def cleanup(dir_path: str):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)


def validate(top_folder: str):

    print('Validating SOURCES to CSR')
    cleanup(out_csr)
    subprocess.run(f'sources2csr {top_folder} {out_csr} {config_dir}', shell=True)

    print('Validating CSR to TRANSMART')
    cleanup(out_tm)
    subprocess.run(f'csr2transmart {out_csr} {out_tm} {config_dir}', shell=True)


if __name__ == '__main__':
    validate('test_data/full_dataset')
