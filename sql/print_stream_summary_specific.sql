SELECT * FROM stream_summary WHERE stream_name LIKE %(stream_name)s ORDER BY message_count DESC;
