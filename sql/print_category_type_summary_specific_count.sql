SELECT COUNT(*) AS total_count FROM messages WHERE category(stream_name) LIKE %(category)s;
