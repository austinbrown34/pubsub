SELECT *
FROM get_category_messages(
    %(category_name)s,
    %(position)s,
    %(batch_size)s,
    correlation => %(correlation)s,
    consumer_group_member => %(consumer_group_member)s,
    consumer_group_size => %(consumer_group_size)s,
    condition => %(condition)s
);
