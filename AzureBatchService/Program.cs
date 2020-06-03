using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection.Metadata;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace AzureBatchService
{
    class Program
    {
        private static string _batchAccountName = "btchdemo";

        private static string _batchAccountKey = "";

        private static string _batchAccountUrl = "";

        private static string _storageAccountName = "";

        private static string _storageAccountKey = "";

        // Batch resource settings
        private const string PoolId = "samplePool";
        private const string JobId = "sampleJob";
        private const int PoolNodeCount = 2;
        private const string PoolVMSize = "STANDARD_A1_v2";

        static void Main(string[] args)
        {
            Console.WriteLine($"Sample started {DateTime.Now}\n");
            Stopwatch timer = new Stopwatch();
            timer.Start();


            // Create the blob client, for use in obtaining references to blob storage containers
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.
                Parse($"DefaultEndpointsProtocol=https;AccountName={_storageAccountName};AccountKey={_storageAccountKey};");

            CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            // Use the blob client to create the input container in Azure Storage 
            const string inputContainerName = "input";

            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(inputContainerName);

            cloudBlobContainer.CreateIfNotExistsAsync().Wait();

            // The collection of data files that are to be processed by the tasks
            List<string> inputFilePaths = new List<string>
                {
                    "taskdata0.txt",
                    "taskdata1.txt",
                    "taskdata2.txt"
                };

            // Upload the data files to Azure Storage. This is the data that will be processed by each of the tasks that are
            // executed on the compute nodes within the pool.
            List<ResourceFile> inputFiles = new List<ResourceFile>();

            foreach (var item in inputFilePaths)
            {
                inputFiles.Add(UploadFileToContainer(cloudBlobClient, inputContainerName, item));
            }

            // Get a Batch client using account creds
            BatchSharedKeyCredentials batchSharedKeyCredentials = new BatchSharedKeyCredentials(_batchAccountUrl, _batchAccountName, _batchAccountKey);

            using (BatchClient batchClient = BatchClient.Open(batchSharedKeyCredentials))
            {
                Console.WriteLine("Creating pool [{0}]...", PoolId);

                // Create a Windows Server image, VM configuration, Batch pool
                ImageReference imageReference = CreateImageReference();

                VirtualMachineConfiguration virtualMachineConfiguration = CreateVirtualMachineConfiguation(imageReference);

                CreateBatchPool(batchClient, virtualMachineConfiguration);

                // Create a Batch job
                Console.WriteLine("Creating job [{0}]...", JobId);

                try
                {
                    CloudJob cloudJob = batchClient.JobOperations.CreateJob();
                    cloudJob.Id = JobId;
                    cloudJob.PoolInformation = new PoolInformation { PoolId = PoolId };
                    cloudJob.Commit();
                }
                catch (BatchException be)
                {
                    // Accept the specific error code JobExists as that is expected if the job already exists
                    if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                    {
                        Console.WriteLine("The job {0} already existed when we tried to create it", JobId);
                    }
                    else
                    {
                        throw; // Any other exception is unexpected
                    }
                }

                // Create a collection to hold the tasks that we'll be adding to the job
                Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, JobId);
                List<CloudTask> tasks = new List<CloudTask>();

                // Create each of the tasks to process one of the input files. 
                for (int i = 0; i < inputFiles.Count; i++)
                {
                    string taskId = String.Format("Task{0}", i);
                    string inputFilename = inputFiles[i].FilePath;
                    string taskCommandLine = String.Format("cmd /c type {0}", inputFilename);

                    CloudTask task = new CloudTask(taskId, taskCommandLine);
                    task.ResourceFiles = new List<ResourceFile>() { inputFiles[i] };

                    tasks.Add(task);

                }

                // Add all tasks to the job.

                batchClient.JobOperations.AddTask(JobId, tasks);

                // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.
                TimeSpan timeout = TimeSpan.FromMinutes(30);
                Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

                IEnumerable<CloudTask> cloudTasks = batchClient.JobOperations.ListTasks(JobId);

                batchClient.Utilities.CreateTaskStateMonitor().WaitAll(cloudTasks, TaskState.Completed, timeout);

                Console.WriteLine("All tasks reached state Completed.");

                // Print task output
                Console.WriteLine();
                Console.WriteLine("Printing task output...");

                IEnumerable<CloudTask> completedtasks = batchClient.JobOperations.ListTasks(JobId);

                foreach (CloudTask task in completedtasks)
                {
                    string nodeId = String.Format(task.ComputeNodeInformation.ComputeNodeId);
                    Console.WriteLine("Task: {0}", task.Id);
                    Console.WriteLine("Node: {0}", nodeId);
                    Console.WriteLine("Standard out:");
                    Console.WriteLine(task.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
                }

                // Print out some timing info
                timer.Stop();
                Console.WriteLine();
                Console.WriteLine("Sample end: {0}", DateTime.Now);
                Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

                // Clean up Batch resources(if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.JobOperations.DeleteJob(JobId);
                }

                Console.Write("Delete pool? [yes] no: ");
                response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.PoolOperations.DeletePool(PoolId);
                }




            }

            Console.ReadLine();
        }

        private static void CreateBatchPool(BatchClient batchClient, VirtualMachineConfiguration vmConfiguration)
        {
            try
            {
                CloudPool cloudPool = batchClient.PoolOperations.CreatePool
                    (
                        poolId: PoolId,
                        targetDedicatedComputeNodes: PoolNodeCount,
                        virtualMachineSize: PoolVMSize,
                        virtualMachineConfiguration: vmConfiguration
                    );


                cloudPool.Commit();
            }
            catch (BatchException be)
            {
                // Accept the specific error code PoolExists as that is expected if the pool already exists
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", PoolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        private static VirtualMachineConfiguration CreateVirtualMachineConfiguation(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(imageReference, "batch.node.windows amd64");
        }

        private static ImageReference CreateImageReference()
        {
            return new ImageReference
            (
                publisher: "MicrosoftWindowsServer",
                offer: "WindowsServer",
                sku: "2016-datacenter-smalldisk",
                version: "latest"
            );
        }

        private static ResourceFile UploadFileToContainer(CloudBlobClient client, string containerName, string filePath)
        {

            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            filePath = Path.Combine(Environment.CurrentDirectory, filePath);

            CloudBlobContainer blobContainer = client.GetContainerReference(containerName);

            CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(blobName);

            blockBlob.UploadFromFileAsync(filePath).Wait();


            // Set the expiry time and permissions for the blob shared access signature. 
            // In this case, no start time is specified, so the shared access signature 
            // becomes valid immediately

            SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2)
            };

            //Get the SAS token

            string token = blobContainer.GetSharedAccessSignature(sharedAccessBlobPolicy);

            string blobUri = $"{blockBlob.Uri}{token}";

            return ResourceFile.FromUrl(blobUri, filePath);

        }

    }
}
