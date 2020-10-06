from analyticsbackend.utils.pipeline import read_config_file
from analyticsbackend.anonymization_pipeline import AnonymizerPipeline
import sys

argv = sys.argv
config_dict = read_config_file(argv[1], argv[2])
AnonymizerPipeline(config_dict).run()
