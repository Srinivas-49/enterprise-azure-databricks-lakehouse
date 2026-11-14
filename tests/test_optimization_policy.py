import pytest
from azure.optimization_policy import choose_join_strategy, choose_partition_column

def test_broadcast_small_vs_large():
    assert choose_join_strategy(1024) == "broadcast"
    assert choose_join_strategy(10*1024*1024) == "sort_merge"

def test_threshold_and_invalid_sizes():
    assert choose_join_strategy(8*1024*1024) == "broadcast"
    with pytest.raises(ValueError): choose_join_strategy(-1)

def test_partition_policy():
    assert choose_partition_column({"order_month","customer_id"}) == "order_month"
    with pytest.raises(ValueError): choose_partition_column({"order_id"})
