SELECT
write_message(
    %(id)s,
    %(stream_name)s,
    %(type)s,
    %(data)s,
    %(metadata)s,
    %(expected_version)s
);
