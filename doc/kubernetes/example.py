import os
import sys
import time
from collections import Counter

import ray


@ray.remote
def get_hostname(x):
    import platform
    import time
    time.sleep(0.01)
    return x + (platform.node(),)


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        num_nodes = len(ray.nodes())
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():
    wait_for_nodes(3)

    # Check that objects can be transferred from each node to each other node.
    for i in range(10):
        print("Iteration {}".format(i))
        results = [
            get_hostname.remote(get_hostname.remote(())) for _ in range(100)
        ]
        print(Counter(ray.get(results)))
        sys.stdout.flush()

    print("Success!")
    sys.stdout.flush()


if __name__ == "__main__":
    # NOTE: If you know you're running this on the head node, you can just use "localhost" here.
    if "RAY_HEAD_IP" not in os.environ or os.environ["RAY_HEAD_IP"] == "":
        raise ValueError("RAY_HEAD_IP environment variable empty. Is there a ray cluster running?")

    ray_head = os.environ["RAY_HEAD_IP"]
    ray.init(address=f"{ray_head}:6379")
    main()
