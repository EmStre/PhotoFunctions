using System;
using System.Collections.Generic;
using System.Text;

namespace HelperClasses
{
    public class Pitstop
    {
        public int PitstopId { get; set; }

        public string PersonId { get; set; }

        public string Title { get; set; }

        public string Note { get; set; }

        public DateTime PitstopDate { get; set; }

        public string PhotoLargeUrl { get; set; }

        public string PhotoMediumUrl { get; set; }

        public string PhotoSmallUrl { get; set; }

        public int TripId { get; set; }

        public string pitstopPosition { get; set; }

        public string Address { get; set; }

        public Pitstop() { }
        public Pitstop(PitstopTableEntity entity)
        {
            this.PitstopId = entity.PitstopId;
            this.PersonId = entity.PersonId;
            this.Title = entity.Title;
            this.Note = entity.Note;
            this.PitstopDate = entity.PitstopDate;
            this.PhotoLargeUrl = entity.PhotoLargeUrl;
            this.PhotoMediumUrl = entity.PhotoMediumUrl;
            this.PhotoSmallUrl = entity.PhotoSmallUrl;
            this.TripId = entity.TripId;
            this.pitstopPosition = entity.pitstopPosition;
            this.Address = entity.Address;
        }

    }
}
