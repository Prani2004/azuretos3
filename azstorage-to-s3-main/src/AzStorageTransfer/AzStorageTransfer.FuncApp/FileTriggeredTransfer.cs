using Amazon.S3;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace AzStorageTransfer.FuncApp
{
    public class FileTriggeredTransfer
    {
        private readonly IAmazonS3 amazonS3;
        private readonly CloudBlobContainer FileExt;

        public FileTriggeredTransfer(IAmazonS3 amazonS3)
        {
            this.amazonS3 = amazonS3;
        }

        [FunctionName(nameof(FileTriggeredTransfer))]
        public async Task Run(
            [BlobTrigger("%LiveContainer%/{name}", Connection = nameof(Config.DataStorageConnection))]
            ICloudBlob myBlob, 
            string name, 
            ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Properties.Length} Bytes");

            using (var ms = new MemoryStream())
            
            {
                // Get the URI of the blob
                var Uri = $"{name}";
                log.LogInformation($"the item url is: {Uri}");

                // Get the file extension required for transfer
                var FileExt = $"{Config.FileExt}";

                // Generate Regex
                var rgxText = @$".*\.{FileExt}";
                log.LogInformation(rgxText);
                Regex rgx = new Regex(rgxText);

                if (rgx.IsMatch(Uri))
                {

                // Download blob content to stream
                await myBlob.DownloadToStreamAsync(ms);                
                ms.Seek(0, SeekOrigin.Begin);
                
                // Upload stream to S3
                await this.amazonS3.UploadObjectFromStreamAsync(Config.Aws.BucketName, name, ms, new Dictionary<string, object>());

                }
                else
                {
                    log.LogInformation($"Not going to transfer and archive this url: {Uri}");
                }
                
            }
        }
    }
}
