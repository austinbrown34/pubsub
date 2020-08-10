SELECT * FROM type_stream_summary WHERE type LIKE %(type)s ORDER BY type, message_count DESC, stream_name;
