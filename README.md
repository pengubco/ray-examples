# AMPC examples on Ray 
[Adaptive Massively Parallel Computation (AMPC) model](https://arxiv.org/abs/1905.07533) is extension of the Massively Parallel Computation (MPC) model. 
Compared to the classic MPC, AMPC has a distributed data store which stores messages sent in a round and all nodes can access the data store in the 
next round. [Ray](https://www.ray.io/) is a distributed computation framework. In this repo, we implement algorithms from the AMPC paper on Ray. 

### Ray cluster setup
Ray version: 2.6.0   
Python Runtime: 3.10

The cluster is setup as follows.

| hostname | IP           |   Node type  |
|----------|--------------|--------------|
| node3    | 192.168.1.16 | Head, Worker |
| node2    | 192.168.1.38 | Worker       |
| node1    | 192.168.1.39 | Worker       |

```shell
# 
pip install ray psycopg2
# On node3. 
ray start --head --node-ip-address=192.168.1.16 --port=6379
# On node1 and node2. 
ray start --address=192.168.1.16:6379
```

## Examples.
1. [Two cycle](./two-cycle/README.md). 
