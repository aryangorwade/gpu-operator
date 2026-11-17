[![license](https://img.shields.io/github/license/NVIDIA/gpu-operator?style=flat-square)](https://raw.githubusercontent.com/NVIDIA/gpu-operator/master/LICENSE)
[![pipeline status](https://github.com/NVIDIA/gpu-operator/actions/workflows/ci.yaml/badge.svg)](https://github.com/NVIDIA/gpu-operator/actions/workflows/ci.yaml)
[![coverage report](https://coveralls.io/repos/github/NVIDIA/gpu-operator/badge.svg?branch=main)](https://coveralls.io/github/NVIDIA/gpu-operator?branch=main)

# NVIDIA GPU Operator

![nvidia-gpu-operator](https://www.nvidia.com/content/dam/en-zz/Solutions/Data-Center/egx/nvidia-egx-platform-gold-image-full-2c50-d@2x.jpg)

Kubernetes provides access to special hardware resources such as NVIDIA GPUs, NICs, Infiniband adapters and other devices through the [device plugin framework](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/). However, configuring and managing nodes with these hardware resources requires configuration of multiple software components such as drivers, container runtimes or other libraries which  are difficult and prone to errors.
The NVIDIA GPU Operator uses the [operator framework](https://cloud.redhat.com/blog/introducing-the-operator-framework) within Kubernetes to automate the management of all NVIDIA software components needed to provision GPU. These components include the NVIDIA drivers (to enable CUDA), Kubernetes device plugin for GPUs, the NVIDIA Container Runtime, automatic node labelling, [DCGM](https://developer.nvidia.com/dcgm) based monitoring and others.

## Audience and Use-Cases
The GPU Operator allows administrators of Kubernetes clusters to manage GPU nodes just like CPU nodes in the cluster. Instead of provisioning a special OS image for GPU nodes, administrators can rely on a standard OS image for both CPU and GPU nodes and then rely on the GPU Operator to provision the required software components for GPUs.

Note that the GPU Operator is specifically useful for scenarios where the Kubernetes cluster needs to scale quickly - for example provisioning additional GPU nodes on the cloud or on-prem and managing the lifecycle of the underlying software components. Since the GPU Operator runs everything as containers including NVIDIA drivers, the administrators can easily swap various components - simply by starting or stopping containers.


## Quick Start

This section provides a quick guide for deploying the GPU Operator with the data center driver.  

Make sure your Kubernetes cluster meets the [prerequisites](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/getting-started.html#prerequisites) and is listed on the [platform support page](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/platform-support.html#supported-operating-systems-and-kubernetes-platforms).


**Step 1: Add the NVIDIA Helm repository**

```bash
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia \
    && helm repo update
```

**Step 2: Deploy GPU Operator**

```bash
helm install --wait --generate-name \
    -n gpu-operator --create-namespace \
    nvidia/gpu-operator
```

After installation, the GPU Operator and its operands should be up and running.

Note:
To deploy the GPU Operator on OpenShift, follow the instructions in the [official documentation](https://docs.nvidia.com/datacenter/cloud-native/openshift/latest/steps-overview.html).


## Product Documentation
For information on platform support and getting started, visit the official documentation [repository](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/overview.html).


## Roadmap

- Support the latest NVIDIA Data Center GPUs, systems, and drivers.
- Support RHEL 10.
- Support KubeVirt with Ubuntu 24.04.
- Promote the [NVIDIADriver](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/gpu-driver-configuration.html) CRD to General Availability (GA).
- Integrate [NVIDIA’s DRA Driver for GPUs](https://github.com/NVIDIA/k8s-dra-driver-gpu) as a managed component of the GPU Operator.

## Webinar
[How to easily use GPUs on Kubernetes](https://info.nvidia.com/how-to-use-gpus-on-kubernetes-webinar.html)

## Contributions
[Read the document on contributions](CONTRIBUTING.md). 
Contributions must adhere to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Support and Getting Help
Please start [a discussion on the GitHub project](https://github.com/NVIDIA/gpu-operator/discussions) for any questions. You can also join our [community Slack](https://inviter.co/dsx-ai-factory). Your feedback is appreciated.

### Collecting diagnostics

On OpenShift, use the GPU Operator image as a must-gather plugin:

```console
oc adm must-gather --image=<gpu-operator-image>
```

The image includes `/usr/bin/gather`, which writes diagnostics to `/must-gather`.
It uses Kubernetes APIs directly; no `kubectl` or `oc` is needed inside the image.
Static Bash is included for the `oc adm must-gather` container launcher, not for
the collector itself. Driver diagnostics still execute the existing tools in
driver pods and require permission to access the `pods/exec` subresource.

For local collection, build and run the same program with your kubeconfig:

```console
make cmd-gather
./gather --dest-dir=./gpu-operator-diagnostics
```

`ARTIFACT_DIR` also sets the output directory. Use `--kubeconfig` to select a
configuration file, `--namespace` if several GPU Operator installations are
present, and `--timeout` to change the default ten-minute collection limit.
Optional APIs and individual log or exec failures are recorded in
`must-gather.stderr.log` without preventing collection of the remaining data.
The existing `hack/must-gather.sh` remains available during the transition.
