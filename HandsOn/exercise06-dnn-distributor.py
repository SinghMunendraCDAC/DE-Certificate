# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 06 : Spark PyTorch Distributor
# MAGIC
# MAGIC In Apache Spark, you can also train neural networks with TensorFlow or PyTorch. Here I'll show you PyTorch training example with Spark PyTorch Distributor.
# MAGIC
# MAGIC Spark PyTorch Distributor (```TorchDistributor```) parallelizes workloads with PyTorch native [distributed data parallel (ddp) implementation](https://pytorch.org/docs/stable/notes/ddp.html) directly on an Apache Spark cluster. You can also bring open source libraries to speed up among open communities, such as, DeepSpeed or Colossal-AI.
# MAGIC
# MAGIC It might be useful to run distributed training on Apache Spark, since you can integrate other operations (such as, data preparation, transformation, evaluations, etc) with the same distributed manners.
# MAGIC
# MAGIC > Note : To use Spark PyTorch Distributor (TorchDistributor), ML Runtime 13.x and above required.<br>
# MAGIC > I recommend to use GPU Runtime in production. (For debugging purpose, you can run on CPU Runtime.)
# MAGIC
# MAGIC > Note : You can also run distributed TensorFlow or PyTorch training on Apache Spark with legacy Horovod runner. See [here](https://tsmatz.github.io/azure-databricks-exercise/exercise06-horovod.html) to run with Horovod Runner.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting, run Exercise 01 (Storage Settings), because we use the mounted folder, ```/mnt/testblob```.

# COMMAND ----------

dbutils.fs.ls("/mnt/testblob")

# COMMAND ----------

# MAGIC %md
# MAGIC In this example, we use the downloaded MNIST dataset and load this data by data-distributed manner with PyTorch ```DistributedSampler```.
# MAGIC
# MAGIC Now we download MNIST dataset into ```/mnt/testblob/dataset``` folder.
# MAGIC
# MAGIC > Note : When you use your own data, please create your own custom dataset to load with ```DistributedSampler```.

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/testblob/dataset")

# COMMAND ----------

from torchvision import datasets, transforms

# set dataset dir
data_path = "/dbfs/mnt/testblob/dataset"

# download MNIST dataset into /mnt/testblob/dataset folder
_ = datasets.MNIST(
  root=data_path,
  train=True,
  download=True,
  transform=transforms.Compose([transforms.ToTensor()]))

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare for the training function, in which we train the model to predict hand-writing digits images.<br>
# MAGIC As I have mentioned above, Spark PyTorch Distributor (TorchDistributor) works together with PyTorch Distributed Data Parallel mechanism (ddp). See [here](https://pytorch.org/tutorials/intermediate/ddp_tutorial.html) for programming PyTorch ddp.
# MAGIC
# MAGIC In this code,
# MAGIC
# MAGIC - TorchDistributor sets in the environment variables ```RANK```, ```WORLD_SIZE``` and ```LOCAL_RANK```. You should then be read via ```os.environ[]``` rather than manually set.
# MAGIC - By using ```DistributedSampler```, each process loads a subset of the original dataset that is exclusive to it.<br>
# MAGIC Dataset is always loaded from local files, because I specify ```download=False``` in ```torchvision.datasets.MNIST()``` as follows.

# COMMAND ----------

import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.optim as optim
import torch.distributed as dist

class MNISTConvNet(nn.Module):
  def __init__(self):
    super(MNISTConvNet, self).__init__()
    self.conv1 = nn.Conv2d(1, 32, 3, 1)
    self.conv2 = nn.Conv2d(32, 64, 3, 1)
    self.dropout = nn.Dropout(0.25)
    self.fc1 = nn.Linear(9216, 128)
    self.fc2 = nn.Linear(128, 10)

  def forward(self, x):
    x = self.conv1(x)
    x = F.relu(x)
    x = self.conv2(x)
    x = F.relu(x)
    x = F.max_pool2d(x, 2)
    x = self.dropout(x)
    x = torch.flatten(x, 1)
    x = self.fc1(x)
    x = F.relu(x)
    x = self.fc2(x)
    output = F.log_softmax(x, dim=1)
    return output

def train_fn(data_path, checkpoint_path, use_gpu=False, learning_rate=0.01, batch_size=64, epochs=5):
  local_rank = int(os.environ["LOCAL_RANK"])
  global_rank = int(os.environ["RANK"])
  world_size = int(os.environ["WORLD_SIZE"])

  if use_gpu:
    dist.init_process_group("nccl", rank=global_rank, world_size=world_size)
  else:
    dist.init_process_group("gloo", rank=global_rank, world_size=world_size)

  device = torch.device("cuda" if use_gpu else "cpu")

  # load data with DistributedSampler
  # (When you load your own data, please create and use custom dataset.)
  train_dataset = datasets.MNIST(
    root=data_path,
    train=True,
    download=False,
    transform=transforms.Compose([transforms.ToTensor()]))
  train_sampler = torch.utils.data.distributed.DistributedSampler(
    train_dataset,
    num_replicas=world_size,
    rank=global_rank
  )
  train_loader = torch.utils.data.DataLoader(
    dataset=train_dataset,
    batch_size=batch_size,
    shuffle=False,
    num_workers=0,
    pin_memory=True,
    sampler=train_sampler)
  
  # load as distributed model
  if use_gpu :
    model = MNISTConvNet().to(local_rank)
    ddp_model = DDP(
      model,
      device_ids=[local_rank],
      output_device=local_rank)
  else:
    model = MNISTConvNet().to(device)
    ddp_model = DDP(model)
  
  # train
  ddp_model.train()
  optimizer = optim.SGD(ddp_model.parameters(), lr=learning_rate)
  for epoch in range(epochs):
    for features, target in train_loader:
      features, target = features.to(device), target.to(device)
      optimizer.zero_grad()
      output = ddp_model(features)
      loss = F.nll_loss(output, target, reduction="mean")
      loss.backward()
      optimizer.step()
   
    if global_rank == 0:
      filepath = checkpoint_path + f"/checkpoint-{epoch + 1}.pt"
      torch.save(ddp_model.module.state_dict(), filepath)

  # save final trained model
  if global_rank == 0:
    filepath = checkpoint_path + "/mnist_cnn.pt"
    torch.save(ddp_model.module.state_dict(), filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC Create checkpoint directory to save the trained model.

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/testblob/checkpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC Run the training with ```TorchDistributor```. (Above function ```train_fn``` will be run on distributed workers.)

# COMMAND ----------

from pyspark.ml.torch.distributor import TorchDistributor 

use_gpu = True if torch.cuda.is_available() else False
checkpoint_path = "/dbfs/mnt/testblob/checkpoint"

distributor = TorchDistributor(
  num_processes=1, # set appropriate number of processes when using GPU
  local_mode=False,
  use_gpu=use_gpu)
output = distributor.run(
  train_fn,
  data_path,
  checkpoint_path,
  use_gpu,
  0.01,
  64,
  5)

# COMMAND ----------

# MAGIC %md
# MAGIC After the training has completed, load the trained model (```mnist_cnn.pt```) and check results.

# COMMAND ----------

import pandas as  pd

# load the trained model
model_for_check = MNISTConvNet().to("cpu")
checkpoint = torch.load(checkpoint_path + "/mnist_cnn.pt")
model_for_check.load_state_dict(checkpoint)

# download MNIST dataset for evaluation
test_dataset = datasets.MNIST(
  root=data_path,
  train=False,
  download=True,
  transform=transforms.Compose([transforms.ToTensor()]))
test_loader = torch.utils.data.DataLoader(
  dataset=test_dataset,
  batch_size=16,
)

# check only 16 records (1 batch)
for features, targets in test_loader:
  with torch.no_grad():
    log_probs = model_for_check(features)
  results = torch.argmax(log_probs, dim=1)
  break

pd.DataFrame(data = {"preds": results.numpy(), "actual": targets.numpy()})

# COMMAND ----------

# MAGIC %md
# MAGIC Remove the trained model and data. (Clean-up)

# COMMAND ----------

dbutils.fs.rm("/mnt/testblob/checkpoint", recurse=True)

# COMMAND ----------

dbutils.fs.rm("/mnt/testblob/dataset", recurse=True)

# COMMAND ----------


