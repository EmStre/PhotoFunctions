using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using HelperClasses;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;

namespace TripPhotoFunction
{
    public static class TripPhotoFunction
    {
        static int SmallPhotoBiggerSide = 270;
        static int BigPhotoBiggerSide = 800;

        [FunctionName("TripPhotoFunction")]
        public static async Task RunAsync([QueueTrigger("tripqueue", Connection = "Storage")]string myQueueItem, ILogger log, ExecutionContext context)
        {
            log.LogInformation($"Resizing image: {myQueueItem}");
            QueueParam item = QueueParam.FromJson(myQueueItem);


            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var storage = config["Storage"];
            var containerName = "photos";
            var tablename = "trip";

            CloudBlobContainer container = GetBlobReference(storage, containerName);

            CloudTable tableTrip = GetTableReference(storage, tablename);

            // Resizing the images
            string smallImageName = await StoreImageAsync(item.PictureUri, container); // method below
            string originalStorageImageName = await ReplaceBigStoreImageAsync(item.PictureUri, container); // method below

            // Updating the trip object in the db
            await UpdateTableImageUrl(item, smallImageName, originalStorageImageName, tableTrip);

            log.LogInformation($"The image resized and saved. Small image name: {smallImageName}, resized original image name: {originalStorageImageName}");


        }

        private static CloudBlobContainer GetBlobReference(string storage, string containerName)
        {
            var storageAccount = CloudStorageAccount.Parse(storage);
            var blobClient = storageAccount.CreateCloudBlobClient();
            return blobClient.GetContainerReference(containerName);
        }

        private static CloudTable GetTableReference(string storage, string tablename)
        {
            var storageAccount = CloudStorageAccount.Parse(storage);
            var tableClient = storageAccount.CreateCloudTableClient();
            return tableClient.GetTableReference(tablename);
        }


        private static async Task<string> StoreImageAsync(string blobName, CloudBlobContainer container)
        {
            CloudBlockBlob pictureBlob = container.GetBlockBlobReference(blobName);
            CloudBlockBlob smallPictureBlob = container.GetBlockBlobReference(Guid.NewGuid().ToString() + ".jpeg");

            // Adding some metadata (connect to the original image)
            smallPictureBlob.Metadata.Add("Type", "small");
            smallPictureBlob.Metadata.Add("Original", blobName);

            // Image resizing
            using (var imageStream = await pictureBlob.OpenReadAsync())
            {
                // Using SixLabors.ImageSharp library here
                Image<Rgba32> originalImage = Image.Load(imageStream);

                originalImage.Size();
                var oldWidth = originalImage.Width;
                var oldHeight = originalImage.Height;

                if ((oldWidth > SmallPhotoBiggerSide) || (oldHeight > SmallPhotoBiggerSide))
                {
                    // checking the ratio + the new size
                    if (originalImage.Width == originalImage.Height)
                    {
                        originalImage.Mutate(x => x.Resize(SmallPhotoBiggerSide, SmallPhotoBiggerSide));
                    }
                    else if (originalImage.Width < originalImage.Height)
                    {
                        var newHeight = SmallPhotoBiggerSide;
                        var newWidth = (newHeight * oldWidth) / oldHeight;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                    else
                    {
                        var newWidth = SmallPhotoBiggerSide;
                        var newHeight = (newWidth * oldHeight) / oldWidth;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                }

                MemoryStream memoStream = new MemoryStream();
                originalImage.SaveAsJpeg(memoStream);
                memoStream.Position = 0;

                await smallPictureBlob.UploadFromStreamAsync(memoStream);
            }

            return smallPictureBlob.Name;
        }

        private static async Task<string> ReplaceBigStoreImageAsync(string blobName, CloudBlobContainer container)
        {
            CloudBlockBlob pictureBlob = container.GetBlockBlobReference(blobName);
            CloudBlockBlob newSizePictureBlob = container.GetBlockBlobReference(Guid.NewGuid().ToString() + ".jpeg");

            // Adding some metadata (connect to the original image)
            newSizePictureBlob.Metadata.Add("Type", "big");

            // Image resizing
            using (var imageStream = await pictureBlob.OpenReadAsync())
            {
                // Using SixLabors.ImageSharp library here
                Image<Rgba32> originalImage = Image.Load(imageStream);

                originalImage.Size();
                var oldWidth = originalImage.Width;
                var oldHeight = originalImage.Height;

                if ((oldWidth > BigPhotoBiggerSide) || (oldHeight > BigPhotoBiggerSide))
                {
                    // checking the ratio + the new size
                    if (originalImage.Width == originalImage.Height)
                    {
                        originalImage.Mutate(x => x.Resize(BigPhotoBiggerSide, BigPhotoBiggerSide));
                    }
                    else if (originalImage.Width < originalImage.Height)
                    {
                        var newHeight = BigPhotoBiggerSide;
                        var newWidth = (newHeight * oldWidth) / oldHeight;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                    else
                    {
                        var newWidth = BigPhotoBiggerSide;
                        var newHeight = (newWidth * oldHeight) / oldWidth;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                }

                MemoryStream memoStream = new MemoryStream();
                originalImage.SaveAsJpeg(memoStream);
                memoStream.Position = 0;

                await newSizePictureBlob.UploadFromStreamAsync(memoStream);
            }

            await pictureBlob.DeleteIfExistsAsync();

            return newSizePictureBlob.Name;
        }

        private static async Task UpdateTableImageUrl(QueueParam item, string smallImageUrl, string bigImageUrl, CloudTable table)
        {
            List<TripTableEntity> tripList = new List<TripTableEntity>();

            int tripId = 0;
            int.TryParse(item.RowKey, out tripId);
    
            if(tripId == 0)
            {
                throw new Exception("Unable to do this...");
            }

            var tripQuery = new TableQuery<TripTableEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, item.PartitionKey));

            TableContinuationToken tokenTrip = null;

            do
            {
                TableQuerySegment<TripTableEntity> resultSegment = await table.ExecuteQuerySegmentedAsync(tripQuery, tokenTrip);
                tokenTrip = resultSegment.ContinuationToken;

                foreach (TripTableEntity entity in resultSegment.Results)
                {
                    if (entity.TripId == tripId)
                        tripList.Add(entity);
                }
            } while (tokenTrip != null);

            var tripToUpdate = tripList.FirstOrDefault(); 
            
            if (tripToUpdate != null)
            {
                tripToUpdate.MainPhotoSmallUrl = smallImageUrl;
                tripToUpdate.MainPhotoUrl = bigImageUrl;

                TableOperation replaceOperation = TableOperation.Replace(tripToUpdate);

                await table.ExecuteAsync(replaceOperation);

            }

        }

    }

}
