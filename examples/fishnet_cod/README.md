# Fishnet

Fishnet stands for **Financial time Series Hosting NETwork**.

It is a Compute-over-Data (CoD) system that uses the distributed Aleph.im network as a substrate for computation.
It is a decentralized, peer-to-peer, and serverless system that allows users to run statistical computations on their
timeseries data without having to upload it to a centralized server.

This python module contains a common data model, built on the
[Aleph Active Record SDK (AARS)](https://github.com/aleph-im/active-record-sdk), that is being used by the Fishnet API
and Executor VMs. The data model is used to store and query:
- Timeseries & Datasets
- Algorithms
- Permissions
- Executions
- Results

Also contains the executor code for the Fishnet Executor VM. Right now it supports Pandas, but in the future it will
support other execution environments (e.g. PyTorch, Tensorflow).

## Deployment
You can deploy your own Fishnet instance using the `deployment` package.

```python
from aleph_client import AuthenticatedUserSession
from fishnet_cod.deployment import deploy_apis, deploy_executors

aleph_session = AuthenticatedUserSession()  # you'll need tons of $ALEPH

executors = deploy_executors(
    executor_path="/your/executor/asgi/app",
    time_slices=[0, -1],  # one executor for all data
    deployer_session=aleph_session,
    channel="MY_DEPLOYMENT_CHANNEL",
)

deploy_apis(executors)
```

## Roadmap

- [x] Basic message model
- [x] API for communicating with Fishnet system
  - [x] Basic CRUD operations
  - [x] Permission management
  - [x] Local VM caching
  - [ ] Signature verification of requests
  - [ ] Discovery of other API instances
  - [ ] Dedicated API deploy function
  - [ ] Timeslice distribution across Executor nodes
- [x] Executor VM
  - [x] Listens for Aleph "Execution" messages and executes them
  - [x] Uploads results to Aleph
  - [x] Pandas support
  - [ ] Dedicated Executor deploy function
  - [ ] Distributed execution & aggregation
    - [x] Discovery of other Executor instances
    - [ ] Uploading executors with metadata: assigned timeslice, code version
  - [ ] Different execution environments (e.g. PyTorch, Tensorflow)
  - [ ] GPU support
- [ ] Versioning and immutable VMs
  - [ ] Automatic Versioning & Deprecation
  - [ ] Version Manifest & Message metadata
  - [ ] Make all deployments immutable
