using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace HelperClasses
{
    public class TripTableEntity : TableEntity
    {
        public int TripId { get; set; }

        public string PersonId { get; set; }

        public string Headline { get; set; }

        public string Description { get; set; }

        public DateTime StartDate { get; set; }

        public DateTime EndDate { get; set; }

        public string MainPhotoUrl { get; set; }

        public string MainPhotoSmallUrl { get; set; }

        public string Position { get; set; }

        public Trip Trip { get; set; }

        public TripTableEntity() { }

        public TripTableEntity(Trip trip)
        {
            this.TripId = trip.TripId;
            this.PersonId = trip.PersonId;
            this.Headline = trip.Headline;
            this.Description = trip.Description;
            this.StartDate = trip.StartDate;
            this.EndDate = trip.EndDate;
            this.MainPhotoUrl = trip.MainPhotoUrl;
            this.MainPhotoSmallUrl = trip.MainPhotoSmallUrl;
            this.Position = trip.Position;
            this.PartitionKey = trip.PersonId;
            this.RowKey = trip.TripId.ToString();
        }
    }
}
