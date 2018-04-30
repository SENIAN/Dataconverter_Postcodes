using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using static System.Diagnostics.Debug;
using Google.Maps;
using Google.Maps.Geocoding;
using MySql.Data.MySqlClient;

namespace DataConverter
{

    //Mapreduce class used as an extension to the program for mapReduce jobs on collections. But not used in this case. 
    public static class MapReduce
    {
        public static IEnumerable<T2> Map<T1, T2>(this IEnumerable<T1> collection, Func<T1, T2> transformation)
        {
            T2[] result = new T2[collection.Count()];
            for (int i = 0; i < collection.Count(); i++)
            {
                result[i] = transformation(collection.ElementAt(i));
            }
            return result;
        }

        public static T2 Reduce<T1, T2>(this IEnumerable<T1> collection, T2 init, Func<T2, T1, T2> operation)
        {
            T2 result = init;
            for (int i = 0; i < collection.Count(); i++)
            {
                result = operation(result, collection.ElementAt(i));
            }
            return result;
        }

        public static IEnumerable<Tuple<T1, T2>> Join<T1, T2>(this IEnumerable<T1> table1, IEnumerable<T2> table2, Func<Tuple<T1, T2>, bool> condition)
        {
            return
              Reduce(table1, new List<Tuple<T1, T2>>(),
                (queryResult, x) =>
                {
                    List<Tuple<T1, T2>> combination =
                Reduce(table2, new List<Tuple<T1, T2>>(),
                        (c, y) =>
                        {
                            Tuple<T1, T2> row = new Tuple<T1, T2>(x, y);
                            if (condition(row))
                                c.Add(row);
                            return c;
                        });
                    queryResult.AddRange(combination);
                    return queryResult;
                });
        }
    }
    class Program
    {
        private static MySqlConnection conn = new MySqlConnection();

        public class LocationDetails
        {
            static string currentLoc;
            public string Postcode { get; set; }
            public string Regio { get; set; }
            public string Plaats { get; set; }
            public double Longitude { get; set; }
            public double Latitude { get; set; }
            public string GooglePlaceId { get; set; }
            public LocationDetails() { }
            public LocationDetails(string postcode, string plaats, string regio, double longitude, double latitude, string googlePlaceId)
            {
                Postcode = postcode;
                Plaats = plaats;
                Regio = regio;
                Longitude = longitude;
                Latitude = latitude;
                GooglePlaceId = googlePlaceId;
            }
        }

        [STAThread]
        static void Main(string[] args)
        {
            conn.Open();
            IEnumerable<LocationDetails> results = GetPostcodeData();
            conn.Close();
        }

        static LocationDetails GetLocationDetails(String postcode)
        {
            LocationDetails locationDetails = new LocationDetails();
            GoogleSigned.AssignAllServices(new GoogleSigned("APIKEY")); // APIKEY van Google hier zetten

            var request = new GeocodingRequest();
            request.Address = postcode;
            {
                var response = new GeocodingService().GetResponse(request);
                if (response.Status == ServiceResponseStatus.OverQueryLimit)
                {
                    WriteLine("API Quotum Reached");

                }

                if (response.Status == ServiceResponseStatus.Ok && response.Results.Count() > 0)
                {
                    var result = response.Results.First();
                    if (result.AddressComponents.Length > 2)
                    {
                        locationDetails = new LocationDetails(
                           result.AddressComponents[0].ShortName,
                           result.AddressComponents[1].LongName,
                           result.AddressComponents[2].LongName,
                           result.Geometry.Location.Longitude,
                           result.Geometry.Location.Latitude,
                           result.PlaceId);

                        Console.WriteLine("Postcode: " +
                            result.AddressComponents[0].LongName + "plaats: " +
                            result.AddressComponents[1].LongName + "regio: " +
                            result.AddressComponents[2].LongName + "," + result.Geometry.Location.Longitude + ", " +
                            result.Geometry.Location.Latitude,
                            result.PlaceId);
                    }
                    else
                    {
                        locationDetails = new LocationDetails(
                          result.AddressComponents[0].ShortName,
                          result.AddressComponents[1].LongName, "",
                          result.Geometry.Location.Longitude,
                          result.Geometry.Location.Latitude,
                          result.PlaceId);
                        Console.WriteLine("Postcode: " +
                          result.AddressComponents[0].LongName + "plaats: " +
                          result.AddressComponents[1].LongName + "," + result.Geometry.Location.Longitude + ", " +
                          result.Geometry.Location.Latitude,
                          result.PlaceId);
                    }

                }
                else
                {
                    Console.WriteLine("Unable to geocode.  Status={0} and ErrorMessage={1}",
                        response.Status, response.ErrorMessage);
                }
                return locationDetails;
            }
        }
        static IEnumerable<LocationDetails> GetPostcodeData()
        {
            // reads the file input from workspace
            List<LocationDetails> locations = new List<LocationDetails>();
            StringReader postcodeReader = new StringReader(DataConverter.PostcodeData.postcodesEnPlaats);
            string line = null;
            string pst = "";
            Queue<string> values = new Queue<string>();
            while ((line = postcodeReader.ReadLine()) != null)
            {
                values.Enqueue(line.Split(';')[0]);
            }
            //enqueues each postcode 
            while ((values.Count != 0))
            {
                string postcodeCheck = values.Dequeue();

                //checks if first 4 numbers of dutch postalcode is not same as the last dequeued version
                //If not the case then make a call to the Google Geo Api and add the result to the database.
                //If it is the same then return the last locationdetail of that postcode , because place is the same no need for call to api
                if (pst != postcodeCheck.Substring(0, 4))
                {
                    LocationDetails location = GetLocationDetails(postcodeCheck);
                    GenerateReport(location);
                    if (location.Postcode != null)
                    {
                        locations.Add(location);
                        GenerateReport(location);
                        pst = location.Postcode.Substring(0, 4);
                    }
                    else if (locations.Count() != 0)
                    {
                        pst = locations.LastOrDefault().Postcode;
                    }
                    else
                    {
                        Console.WriteLine("API Quotum Reached");
                        break;
                    }
                }
                // first condition
                else if (locations.Count() != 0)
                {
                   LocationDetails location = locations.LastOrDefault();
                    location.Postcode = postcodeCheck;
                    locations.Add(location);
                    GenerateReport(location);
                }
                else
                {
                    Console.WriteLine("API Quotum Reached");
                    break;
                }

            }



            return locations;
        }
        //insert into the database
        static void GenerateReport(LocationDetails x)
        {

            MySqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT Into PostcodePlaats (Postcode, Plaats, Regio, Longitude, Latitude, GooglePlaceId) VALUES(@postcode, @plaats, @regio, @longitude, @latitude,@googleplaceId)";
            cmd.Parameters.AddWithValue("@postcode", x.Postcode);
            cmd.Parameters.AddWithValue("@plaats", x.Plaats);
            cmd.Parameters.AddWithValue("@regio", x.Regio);
            cmd.Parameters.AddWithValue("@longitude", x.Longitude);
            cmd.Parameters.AddWithValue("@latitude", x.Latitude);
            cmd.Parameters.AddWithValue("@googleplaceId", x.GooglePlaceId);
  
            cmd.ExecuteNonQuery();
        }

    }
}


