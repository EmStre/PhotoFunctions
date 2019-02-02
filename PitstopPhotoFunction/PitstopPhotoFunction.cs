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

namespace PitstopPhotoFunction
{
    public static class PitstopPhotoFunction
    {
        static int LargePhotoBiggerSide = 800;
        static int MediumPhotoBiggerSide = 500;
        static int SmallPhotoBiggerSide = 270;

        [FunctionName("PitstopPhotoFunction")]
        public static async Task RunAsync([QueueTrigger("pitstopqueue", Connection = "Storage")]string myQueueItem, ILogger log, ExecutionContext context)
        {
            log.LogInformation($"Resizing pitstop image: {myQueueItem}");
            QueueParam item = QueueParam.FromJson(myQueueItem);

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var storage = config["Storage"];
            var containerName = "photos";
            var tablename = "pitstop";

            CloudBlobContainer container = GetBlobReference(storage, containerName);

            CloudTable tablePitstop = GetTableReference(storage, tablename);

            // Resizing the image and naming the images
            string smallImageName = await StoreSmallImage(item.PictureUri, container); // method below
            string mediumStorageImageName = await StoreMediumImage(item.PictureUri, container); // method below
            string originalStorageImageName = await ReplaceLargeStoreImageAsync(item.PictureUri, container); // method below

            // Updating the trip object in the db
            await UpdateDocumentTableWithUriAsync(item, smallImageName, mediumStorageImageName, originalStorageImageName, tablePitstop);

            log.LogInformation($"The image resized and saved. Small image name: {smallImageName}, medium image name: {mediumStorageImageName}, large image name: {originalStorageImageName}");

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

        private static async Task<string> StoreSmallImage(string blobName, CloudBlobContainer container)
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

        private static async Task<string> StoreMediumImage(string blobName, CloudBlobContainer container)
        {
            CloudBlockBlob pictureBlob = container.GetBlockBlobReference(blobName);
            CloudBlockBlob mediumPictureBlob = container.GetBlockBlobReference(Guid.NewGuid().ToString() + ".jpeg");

            // Adding some metadata (connect to the original image)
            mediumPictureBlob.Metadata.Add("Type", "medium");

            // Image resizing
            using (var imageStream = await pictureBlob.OpenReadAsync())
            {
                // Using SixLabors.ImageSharp library here
                Image<Rgba32> originalImage = Image.Load(imageStream);

                originalImage.Size();
                var oldWidth = originalImage.Width;
                var oldHeight = originalImage.Height;

                if ((oldWidth > MediumPhotoBiggerSide) || (oldHeight > MediumPhotoBiggerSide))
                {
                    // checking the ratio + the new size
                    if (originalImage.Width == originalImage.Height)
                    {
                        originalImage.Mutate(x => x.Resize(MediumPhotoBiggerSide, MediumPhotoBiggerSide));
                    }
                    else if (originalImage.Width < originalImage.Height)
                    {
                        var newHeight = MediumPhotoBiggerSide;
                        var newWidth = (newHeight * oldWidth) / oldHeight;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                    else
                    {
                        var newWidth = MediumPhotoBiggerSide;
                        var newHeight = (newWidth * oldHeight) / oldWidth;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                }

                MemoryStream memoStream = new MemoryStream();
                originalImage.SaveAsJpeg(memoStream);
                memoStream.Position = 0;

                await mediumPictureBlob.UploadFromStreamAsync(memoStream);
            }
            return mediumPictureBlob.Name;
        }

        private static async Task<string> ReplaceLargeStoreImageAsync(string blobName, CloudBlobContainer container)
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

                var oldWidth = originalImage.Width;
                var oldHeight = originalImage.Height;

                if ((oldWidth > LargePhotoBiggerSide) || (oldHeight > LargePhotoBiggerSide))
                {
                    // checking the ratio + the new size
                    if (originalImage.Width == originalImage.Height)
                    {
                        originalImage.Mutate(x => x.Resize(LargePhotoBiggerSide, LargePhotoBiggerSide));
                    }
                    else if (originalImage.Width < originalImage.Height)
                    {
                        var newHeight = LargePhotoBiggerSide;
                        var newWidth = (newHeight * oldWidth) / oldHeight;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                    else
                    {
                        var newWidth = LargePhotoBiggerSide;
                        var newHeight = (newWidth * oldHeight) / oldWidth;

                        originalImage.Mutate(x => x.Resize(newWidth, newHeight));
                    }
                }

                MemoryStream memoStream = new MemoryStream();
                originalImage.SaveAsJpeg(memoStream);
                memoStream.Position = 0;

                await newSizePictureBlob.UploadFromStreamAsync(memoStream);
            }

            // Deleting the original image in case it is really big
            await pictureBlob.DeleteIfExistsAsync();

            return newSizePictureBlob.Name;
        }

        private static async Task UpdateDocumentTableWithUriAsync(QueueParam item, string smallImageName, string mediumStorageImageName, string originalStorageImageName, CloudTable tablePitstop)
        {
            List<PitstopTableEntity> pitstopList = new List<PitstopTableEntity>();
            
            int pitstopId = 0;
            int.TryParse(item.RowKey, out pitstopId);

            if (pitstopId == 0)
            {
                throw new Exception("Unable to do this...");
            }

            var pitstopQuery = new TableQuery<PitstopTableEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, item.PartitionKey));

            TableContinuationToken tokenPitstop = null;

            do
            {
                TableQuerySegment<PitstopTableEntity> resultSegment = await tablePitstop.ExecuteQuerySegmentedAsync(pitstopQuery, tokenPitstop);
                tokenPitstop = resultSegment.ContinuationToken;

                foreach (PitstopTableEntity entity in resultSegment.Results)
                {
                    if (entity.PitstopId == pitstopId)
                        pitstopList.Add(entity);
                }
            } while (tokenPitstop != null);

            var pitstopToUpdate = pitstopList.FirstOrDefault();

            if (pitstopToUpdate != null)
            {
                pitstopToUpdate.PhotoSmallUrl = smallImageName;
                pitstopToUpdate.PhotoMediumUrl = mediumStorageImageName;
                pitstopToUpdate.PhotoLargeUrl = originalStorageImageName;

                TableOperation replaceOperation = TableOperation.Replace(pitstopToUpdate);

                await tablePitstop.ExecuteAsync(replaceOperation);

            }

        }
    }
}
