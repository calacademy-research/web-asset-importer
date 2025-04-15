import os
import sys
PROJECT_PATH = os.getcwd()
IMAGE_CLIENT_PATH = os.path.join(PROJECT_PATH, "image_client")
IZ_IMPORTER_TESTS_PATH = os.path.join(PROJECT_PATH, "tests", "iz_importer")

sys.path.append(IMAGE_CLIENT_PATH)
sys.path.append(IZ_IMPORTER_TESTS_PATH)