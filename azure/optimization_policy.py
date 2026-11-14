"""Spark query tuning decisions; measure effects on a real cluster before making performance claims."""
def choose_join_strategy(dimension_size_bytes, broadcast_max_bytes=8*1024*1024):
    if dimension_size_bytes < 0 or broadcast_max_bytes <= 0:
        raise ValueError("Invalid size or threshold")
    return "broadcast" if dimension_size_bytes <= broadcast_max_bytes else "sort_merge"

def choose_partition_column(columns):
    if "order_month" not in columns:
        raise ValueError("order_month is required")
    return "order_month"
