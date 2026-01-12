from config import load_config

cfg = load_config()
print(cfg.cdc.topics)
print(cfg.ops.metadata_db_path)
print(cfg.output.bronze_base_path)
