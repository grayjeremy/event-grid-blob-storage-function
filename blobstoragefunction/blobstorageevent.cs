// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGridExtensionConfig?functionName={functionname}


using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace blobstoragefunction
{
    public static class blobstorageevent
    {
        [FunctionName("blobstorageevent")]
        public static void Run([EventGridTrigger]EventGridEvent eventGridEvent, TraceWriter log)
        {
            log.Info(eventGridEvent.Data.ToString());

            string sourceStorageConnectionString = "**Add to key vault**";
            string destinationStorageConnectionString = "**Add to key vault**";
            string containerName = ContainerNameFromSubject(eventGridEvent.Subject);
            string archiveContainerName = containerName;
            string fileName = BlobNameFromSubject(eventGridEvent.Subject);

            log.Info($"Container is: {containerName}");
            log.Info($"Archive Container is: {archiveContainerName}");
            log.Info($"Blob name is: {fileName}");

            CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(sourceStorageConnectionString);
            CloudBlobClient sourceStorageClient = sourceAccount.CreateCloudBlobClient();
            var sourceContainer = sourceStorageClient.GetContainerReference(containerName);
            CreateContainerIfNotExists(log, sourceContainer);

            CloudStorageAccount destinationAccount = CloudStorageAccount.Parse(destinationStorageConnectionString);
            CloudBlobClient destinationStorageClient = destinationAccount.CreateCloudBlobClient();
            var destinationContainer = destinationStorageClient.GetContainerReference(archiveContainerName);
            CreateContainerIfNotExists(log, destinationContainer);


            log.Info($"EventType: {eventGridEvent.EventType}");
            if (eventGridEvent.EventType == "Microsoft.Storage.BlobCreated")
            {
                CreateBlobInArchive(sourceContainer, fileName, destinationContainer, log);
            }
            else if (eventGridEvent.EventType == "Microsoft.Storage.BlobDeleted")
            {
                DeleteBlobInArchive();
            }
        }
        private static void CreateContainerIfNotExists(TraceWriter log, CloudBlobContainer destinationContainer)
        {
            try
            {
                var r = destinationContainer.CreateIfNotExistsAsync().Result;
            }
            catch (Exception e)
            {
                log.Error(e.Message);
            }
        }

        private static void DeleteBlobInArchive()
        {
            throw new NotImplementedException();
        }

        private static void CreateBlobInArchive(CloudBlobContainer sourceContainer, string fileName, CloudBlobContainer destinationContainer, TraceWriter log)
        {
            // TODO: register the function as a MSI so I don't need a SAS token


            CloudBlockBlob sourceBlobVersionCheck = sourceContainer.GetBlockBlobReference(fileName);            
            sourceBlobVersionCheck.FetchAttributesAsync().Wait();
            DateTime lastModDT = Convert.ToDateTime(sourceBlobVersionCheck.Properties.LastModified.ToString());
            string lastModified = lastModDT.ToString("yyyyMMdd-HHmmsszz");
            string versionedFileName = fileName + "." + lastModified;

            CloudBlockBlob sourceBlob = sourceContainer.GetBlockBlobReference(fileName);
            CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(fileName);
            CloudBlockBlob destinationArchiveBlob = destinationContainer.GetBlockBlobReference(versionedFileName);

            var existsTask = sourceBlob.ExistsAsync();
            existsTask.Wait();

            if (existsTask.Result)
            {
                log.Info($"Copying {sourceBlob.Uri.ToString()} to {destinationBlob.Uri.ToString()}");

                try
                {
                    destinationBlob.StartCopyAsync(sourceBlob).Wait();
                }
                catch (Exception e)
                {
                    log.Error(e.Message);
                }

                log.Info($"Copying {sourceBlob.Uri.ToString()} to {destinationArchiveBlob.Uri.ToString()}");
                try
                {
                    destinationArchiveBlob.StartCopyAsync(sourceBlob).Wait();
                }
                catch (Exception e)
                {
                    log.Error(e.Message);
                }
            }
            else
            {
                log.Info("Source blob does not exist, no copy made");
            }

        }

        private static string ContainerNameFromSubject(string subject)
        {
            string[] subjectSplit = subject.Split('/');
            int containerPosition = Array.IndexOf(subjectSplit, "containers");
            string container = subjectSplit[containerPosition + 1];

            return container;
        }

        private static string BlobNameFromSubject(string subject)
        {
            string[] subjectSplit = subject.Split('/');
            int blobPosition = Array.IndexOf(subjectSplit, "blobs");
            string fileName = subjectSplit[blobPosition + 1];
            return fileName;
        }
    }
}
