using Amazon.S3;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Text.RegularExpressions;


namespace AzStorageTransfer.FuncApp
{
    public class ScheduledTransfer
    {
        /// <summary>
        /// Cron expression to schedule execution.
        /// </summary>
        private const string CronSchedule = "0 */5 * * * *";

        /// <summary>
        /// Amazon S3 client.
        /// </summary>
        private readonly IAmazonS3 amazonS3;
        private readonly CloudBlobClient cloudBlobClient;
        private readonly CloudBlobContainer scheduledBlobContainer;
        private readonly CloudBlobContainer archiveBlobContainer;
        private readonly CloudBlobContainer ScheduledVirtualPath;
        private readonly CloudBlobContainer FileExt;

        public ScheduledTransfer(IAmazonS3 amazonS3)
        {
            this.amazonS3 = amazonS3;
            this.cloudBlobClient = CloudStorageAccount.Parse(Config.DataStorageConnection).CreateCloudBlobClient();
            this.scheduledBlobContainer = this.cloudBlobClient.GetContainerReference(Config.ScheduledContainer);
            this.archiveBlobContainer = this.cloudBlobClient.GetContainerReference(Config.ArchiveContainer);
        }

        /// <summary>
        /// Scheduled copy of files from Az blob container 'scheduled' to S3 and then moved to an archive container.
        /// </summary>
        [FunctionName(nameof(ScheduledTransfer))]
        public async Task Run([TimerTrigger(CronSchedule, RunOnStartup = false)] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var blobItems = scheduledBlobContainer.ListBlobs(useFlatBlobListing: true, prefix: Config.ScheduledVirtualPath);
            
            foreach (CloudBlockBlob item in blobItems)
            try
            {
                    await TrasferAndArchiveBlobAsync(item, log);
            }
            catch (Exception e)
            {
                log.LogInformation($"failed on 60 - {e.Message}");
            }
        }

        /// <summary>
        /// Exposes the same functionality described in the ScheduledTranfer but via an HttpTrigger.
        /// Used to integrate with Data Factory if needed.
        /// </summary>
        [FunctionName(nameof(TransferFiles))]
        public async Task<IActionResult> TransferFiles(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "tranferfiles")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            var blobItems = scheduledBlobContainer.ListBlobs(useFlatBlobListing: true, prefix: Config.ScheduledVirtualPath);
            foreach (CloudBlockBlob item in blobItems)
            try
            {     
                    await TrasferAndArchiveBlobAsync(item, log);
            }
            catch (Exception e)
            {
                log.LogInformation($"failed on 82 - {e.Message}");
            }

            return new OkResult();
        }

        private async Task TrasferAndArchiveBlobAsync(CloudBlockBlob cloudBlob, ILogger log)
        {
            using (var ms = new MemoryStream())
            {
                // Get the URI of the blob
                var Uri = $"{cloudBlob.Uri}";
                log.LogInformation($"the item url is: {Uri}");

                // Get the file extension required for transfer
                var FileExt = $"{Config.FileExt}";

                // Generate Regex
                var rgxText = @$".*\.{FileExt}";
                
                log.LogInformation($"Validating file type using regex pattern: {rgxText});
                                   
                Regex rgx = new Regex(rgxText);

                // Only send files that match the configured file extension
                if (rgx.IsMatch(Uri))
                {
                
                // Download to stream
                try
                {
                    await cloudBlob.DownloadToStreamAsync(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                }
                catch (Exception e)
                {
                    log.LogInformation($"failed downloading blob to stream - {e.Message}");
                }
                

                // Transfer to Amazon S3
                try
                {
                    await this.amazonS3.UploadObjectFromStreamAsync(Config.Aws.BucketName, cloudBlob.Name, ms, new Dictionary<string, object>());
                    log.LogInformation($"File '{cloudBlob.Name}' uploaded to S3.");
                }
                catch (Exception e)
                {
                    log.LogInformation($"failed transfering blob to S3 - {e.Message}");
                }
                

                // Copy file to archive container
                log.LogInformation($"Starting Archive process..");
                var archiveBlob = this.archiveBlobContainer.GetBlockBlobReference(cloudBlob.Name);
                log.LogInformation($"Starting Copy Async..");
                try
                {
                    var copyResult = await archiveBlob.StartCopyAsync(cloudBlob);
                    log.LogInformation($"File '{cloudBlob.Name}' copied to container: {Config.ArchiveContainer}.");
                }
                catch (Exception e)
                {
                    log.LogInformation($"failed copying file to archive container - {e.Message}");
                }

                // Delete file from scheduled container
                try
                {
                    await cloudBlob.DeleteAsync();
                    log.LogInformation($"File '{cloudBlob.Name}' deleted from container: {Config.ScheduledContainer}.");
                }
                catch (Exception e)
                {
                    log.LogInformation($"failed deleting file from scheduled container - {e.Message}");
                }
                

                }
                else
                {
                    log.LogInformation($"Not going to transfer and archive this url: {Uri}");
                }
                
            }

            
        }
    }
}
