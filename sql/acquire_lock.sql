SELECT *
FROM acquire_lock(
    %(stream_name)s
);
