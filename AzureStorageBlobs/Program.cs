using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;


namespace AzureStorageBlobs
{
    class Program
    {
        static string _strStorageConnection = "COPY_YOUR_CONNECTION_STRING_HERE";
        static string _containerName = "myblobcontainer3";

        static void Main(string[] args)
        {
            // Get Reference to the storage account using the connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_strStorageConnection);
            // Create a client-side logical representation of Microsoft Azure Blob storage
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer cloudBlobContainer = CreateContainer(blobClient, _containerName).GetAwaiter().GetResult();
            UploadBlob(cloudBlobContainer, "myBlob", @"D:\uploadblob").GetAwaiter().GetResult();
            ListBlobs(blobClient, _containerName).GetAwaiter().GetResult();
            DownloadBlob(cloudBlobContainer, "myBlob", @"D:\downloadblob").GetAwaiter().GetResult();
            DeleteBlob(cloudBlobContainer, "myBlob").GetAwaiter().GetResult();
            ListBlobs(blobClient, "myblobcontainer2").GetAwaiter().GetResult();
            CopyBlob(blobClient, "myblobcontainer1", "myBlob", "myblobcontainer2", "myBlob_copy").GetAwaiter().GetResult();
            ListBlobs(blobClient, "myblobcontainer2").GetAwaiter().GetResult();
        }


        static async Task<CloudBlobContainer> CreateContainer(CloudBlobClient blobClient, string containerName)
        {
            // Get Container reference
            CloudBlobContainer blobContainer = blobClient.GetContainerReference(containerName);
            try
            {

                // We can also use CreateIfNotExistsAsync method, however it doesn't convey the information if the container
                // already exists. CreateAsync on the other hand throws an exception if the container already exists.
                await blobContainer.CreateAsync();
                }
            catch (Exception ex)
            {
                Console.WriteLine("{0}: {1}",containerName, ex.Message);
            }
            Console.WriteLine();
            return blobContainer;
        }

        static async Task UploadBlob(CloudBlobContainer blobContainer, string blobName, string blobPath)
        {
            // Get Blob reference using the container reference previously created
            CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(blobName);
            try
            {
                // Check if path exists
                if (File.Exists(blobPath))
                {
                    // Upload BlockBlob
                    await blockBlob.UploadFromFileAsync(blobPath);
                    Console.WriteLine("Blob {0} uploaded successfully", blobName);
                }
                else
                    Console.WriteLine("Invalid file location specified.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine();
        }

        static async Task DownloadBlob(CloudBlobContainer blobContainer, string blobName, string blobPath)
        {
            // Get Blob reference using the container reference previously created
            CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(blobName);
            try
            {
                // Download Blob
                await blockBlob.DownloadToFileAsync(blobPath,FileMode.Create);
                Console.WriteLine("Blob {0} downloaded successfully", blobName);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine();
        }

        static async Task DeleteBlob(CloudBlobContainer blobContainer, string blobName)
        {
            // Get Blob reference using the container reference previously created
            CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(blobName);
            try
            {
                // Delete Blob
                await blockBlob.DeleteAsync();
                Console.WriteLine("Blob {0} deleted successfully from container {1}", blobName, blobContainer.Name);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine();
        }

        static async Task CopyBlob(CloudBlobClient blobClient, string fromContainerName, string fromBlobName, string toContainerName, string toBlobName)
        {
            // Get the source container reference to copy the blob from
            CloudBlobContainer fromCloudBlobContainer = CreateContainer(blobClient, fromContainerName).GetAwaiter().GetResult();
            // Get the source blob reference
            CloudBlockBlob fromBlockBlob = fromCloudBlobContainer.GetBlockBlobReference(fromBlobName);

            // Get the destination container reference to copy the blob to
            CloudBlobContainer toCloudBlobContainer = CreateContainer(blobClient, toContainerName).GetAwaiter().GetResult();
            // Get the destination blob reference
            CloudBlockBlob toBlockBlob = toCloudBlobContainer.GetBlockBlobReference(toBlobName);

            // Copy the blob
            await toBlockBlob.StartCopyAsync(new Uri(fromBlockBlob.Uri.AbsoluteUri));

        }

        static async Task ListBlobs(CloudBlobClient blobClient, string containerName)
        {
            // Get Container reference
            CloudBlobContainer blobContainer = blobClient.GetContainerReference(containerName);
            List<IListBlobItem> lstBlobs = new List<IListBlobItem>();
            BlobContinuationToken continuationToken = null;
            
            // Get the list of blobs in the container and store them into a list
            do
            {
                try
                {
                    // Get list of blobs
                    BlobResultSegment resultSegment = await blobContainer.ListBlobsSegmentedAsync(continuationToken);
                    // Update continuation token
                    continuationToken = resultSegment.ContinuationToken;
                    // Add Blobs to the list
                    lstBlobs.AddRange(resultSegment.Results);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            } while (continuationToken != null);

            // Print Blob information
            Console.WriteLine("Listing blobs for Container {0}",containerName);
            Console.WriteLine("+++++++++++++++++++++++++++++++++++++++++++++++++++");
            if (lstBlobs.Count == 0)
            {
                Console.WriteLine("No blobs found");
            }
            else
            {
                foreach (IListBlobItem item in lstBlobs)
                {
                    // We are working with CloudBlockBlobs only here
                    if (item.GetType() == typeof(CloudBlockBlob))
                        Console.WriteLine("Blob Name={0}, URI={1}", ((CloudBlockBlob)item).Name, ((CloudBlockBlob)item).Uri);
                }
            }
            Console.WriteLine("+++++++++++++++++++++++++++++++++++++++++++++++++++");
            Console.WriteLine();
        }
    }
}
