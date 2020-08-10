SELECT *
FROM get_stream_messages(
    %(stream_name)s,
    %(position)s,
    %(batch_size)s,
    condition => %(condition)s
);
