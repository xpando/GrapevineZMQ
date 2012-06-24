using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using Grapevine.Client;
using System.Diagnostics;
using System.Threading;

namespace GrapevineBreaker
{

    public struct Sums
    {
        public long Sum { get; set; }
        public int Number { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            int numClients = 100;
            int nn = numClients * 100;
            long expectedTotal = nn * (nn + 1) / 2;

            var subClient = new GrapevineClient("tcp://localhost:5560", "tcp://localhost:5559") as IGrapevineClient;
            var rObs = subClient.Receive<MyMessage>(m => true)
                .SubscribeOn(Scheduler.TaskPool)
                .Scan(new Sums { Sum = 0, Number = 0 }, (o, n) =>
                {
                    //o.Number = n.Number;
                    //o.Sum = o.Sum + n.Number;
                    //return o;
                    return new Sums { Number = n.Number, Sum = o.Sum + n.Number };
                })
                //  .Take(nn)
                .Select(n => "Received: " + n.Number + " Sum: " + n.Sum + " Expected: " + expectedTotal);

            //    .Subscribe(n => Console.WriteLine("Received: " + n.Number + " Sum: " + n.Sum + " Expected: " + expectedTotal), ex => Console.WriteLine(ex.ToString()));




            var clients = Enumerable.Range(1, numClients).Select(_ => subClient as IGrapevineClient);
            var numbers = Enumerable.Range(1, 100 * numClients).Chunk(100);

            var clientsAndNumbers = clients.Zip(numbers, (c, n) => new { Client = c, Numbers = n.ToObservable() });

            var clientNumberPairs = clientsAndNumbers.ToObservable();




            var sObs = clientNumberPairs
                .SelectMany(cnp => Observable.Start(() =>
                {
                    return cnp.Numbers.SelectMany(n => Observable.Start(() =>
                    {
                        cnp
                            .
                            Client
                            .
                            Send
                            (new MyMessage
                                 ()
                            {
                                Number
                                    =
                                    n
                            });
                        return
                            n;
                    },
                Scheduler.TaskPool).Delay(TimeSpan.FromMilliseconds(1)));
                }, Scheduler.TaskPool))
                .Merge(numClients)
                .Select(n => n.ToString());


            var mObs = sObs.Merge(rObs)
                .Subscribe(s =>
                {
                    Console.WriteLine(s);
                }, ex => {
                    Console.WriteLine(ex.ToString());
                });






            WaitForKey();

            mObs.Dispose();

            WaitForKey();
        }

        public static void WaitForKey()
        {
            while (!Console.KeyAvailable)
            {
                Thread.Yield();
            }
            Console.ReadKey();
        }
    }

    [DataContract]
    public class MyMessage
    {
        [DataMember(Order=1)]
        public int Number { get; set; }

    }

    public static class EnumerableMixins
    {
        /// <summary>
        /// Break a list of items into chunks of a specific size
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
        {
            while (source.Any())
            {
                yield return source.Take(chunksize);
                source = source.Skip(chunksize);
            }
        }

    }
}