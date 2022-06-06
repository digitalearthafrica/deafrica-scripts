from deafrica.monitoring import latency_check
import pytest


def test_latency_checker():
    assert latency_check("asdasd", 3, None) == -1
    assert latency_check("s2_l2a", -1, None) == -1
    assert latency_check("s2_l2a", 3, None) == 0
