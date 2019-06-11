from pathlib import Path

# Project root dir
ROOT_DIR = Path(__file__).parent.resolve()

CONFIG_DIR = ROOT_DIR.joinpath('config')

TEST_DATA_DIR = ROOT_DIR.joinpath('test_data')
TESTS_DIR = ROOT_DIR.joinpath('tests')
TEST_EXPECTED_OUT_DIR = TESTS_DIR.joinpath('expected_out')
