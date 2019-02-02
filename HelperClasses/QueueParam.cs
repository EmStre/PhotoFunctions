using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace HelperClasses
{
    public class QueueParam
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string PictureUri { get; set; }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }

        public static QueueParam FromJson(string json)
        {
            return JsonConvert.DeserializeObject<QueueParam>(json);
        }
    }
}
