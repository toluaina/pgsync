ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_slot_wal_keep_size = 10GB;
ALTER SYSTEM SET max_replication_slots = 2;
ALTER SYSTEM SET max_wal_senders = 2;
