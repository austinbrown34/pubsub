SELECT * FROM stream_type_summary WHERE stream_name LIKE %(stream_name)s ORDER BY stream_name, message_count DESC;
