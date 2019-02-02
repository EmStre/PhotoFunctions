using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace HelperClasses
{

    public class PitstopTableEntity : TableEntity
    {
        public int PitstopId { get; set; }

        public int TripId { get; set; }

        public string PersonId { get; set; }

        public string Title { get; set; }

        public string Note { get; set; }

        public DateTime PitstopDate { get; set; }

        public string PhotoLargeUrl { get; set; }

        public string PhotoMediumUrl { get; set; }

        public string PhotoSmallUrl { get; set; }

        public string pitstopPosition { get; set; }

        public string Address { get; set; }

        public Pitstop Pitstop { get; set; }

        public PitstopTableEntity() { }

        public PitstopTableEntity(Pitstop pitstop)
        {
            this.PitstopId = pitstop.PitstopId;
            this.TripId = pitstop.TripId;
            this.PersonId = pitstop.PersonId;
            this.Title = pitstop.Title;
            this.Note = pitstop.Note;
            this.PitstopDate = pitstop.PitstopDate;
            this.PhotoLargeUrl = pitstop.PhotoLargeUrl;
            this.PhotoMediumUrl = pitstop.PhotoMediumUrl;
            this.PhotoSmallUrl = pitstop.PhotoSmallUrl;
            this.pitstopPosition = pitstop.pitstopPosition;
            this.Address = pitstop.Address;
            this.PartitionKey = pitstop.TripId.ToString() + ";" + pitstop.PersonId;
            this.RowKey = pitstop.PitstopId.ToString();

        }
    }
}
