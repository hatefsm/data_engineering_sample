from config import load_config
from metadata.sqlite_store import SQLiteMetadataStore

cfg = load_config()
m = SQLiteMetadataStore(cfg.ops.metadata_db_path)

run_id = "test_run_meta_001"
m.start_run(run_id)
m.upsert_checkpoint("topicA", 0, 123)
m.commit()
m.end_run(run_id, "SUCCESS", {"ok": True})
m.close()

print("metadata ok")
